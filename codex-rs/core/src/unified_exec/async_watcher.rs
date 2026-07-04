use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::Sleep;

use super::UnifiedExecContext;
use super::process::UnifiedExecProcess;
use crate::exec::MAX_EXEC_OUTPUT_DELTAS_PER_CALL;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;
use crate::tools::events::ToolEmitter;
use crate::tools::events::ToolEventCtx;
use crate::tools::events::ToolEventFailure;
use crate::tools::events::ToolEventStage;
use crate::unified_exec::head_tail_buffer::HeadTailBuffer;
use codex_protocol::exec_output::ExecToolCallOutput;
use codex_protocol::exec_output::StreamOutput;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecCommandOutputDeltaEvent;
use codex_protocol::protocol::ExecCommandSource;
use codex_protocol::protocol::ExecOutputStream;
use codex_utils_path_uri::PathUri;

pub(crate) const TRAILING_OUTPUT_GRACE: Duration = Duration::from_millis(100);

/// Upper bound for a single ExecCommandOutputDelta chunk emitted by unified exec.
///
/// The unified exec output buffer already caps *retained* output (see
/// `UNIFIED_EXEC_OUTPUT_MAX_BYTES`), but we also cap per-event payload size so
/// downstream event consumers (especially app-server JSON-RPC) don't have to
/// process arbitrarily large delta payloads.
const UNIFIED_EXEC_OUTPUT_DELTA_MAX_BYTES: usize = 8192;

/// Spawn a background task that continuously reads from the PTY, appends to the
/// shared transcript, and emits ExecCommandOutputDelta events on UTF‑8
/// boundaries.
pub(crate) fn start_streaming_output(
    process: &UnifiedExecProcess,
    context: &UnifiedExecContext,
    transcript: Arc<Mutex<HeadTailBuffer>>,
) {
    let mut receiver = process.output_receiver();
    let output_drained = process.output_drained_notify();
    let exit_token = process.cancellation_token();

    let session_ref = Arc::clone(&context.session);
    let turn_ref = Arc::clone(&context.turn);
    let call_id = context.call_id.clone();

    tokio::spawn(async move {
        use tokio::sync::broadcast::error::RecvError;

        let mut pending = Vec::<u8>::new();
        let mut emitted_deltas: usize = 0;

        let mut grace_sleep: Option<Pin<Box<Sleep>>> = None;

        loop {
            tokio::select! {
                _ = exit_token.cancelled(), if grace_sleep.is_none() => {
                    let deadline = Instant::now() + TRAILING_OUTPUT_GRACE;
                    grace_sleep.replace(Box::pin(tokio::time::sleep_until(deadline)));
                }

                _ = async {
                    if let Some(sleep) = grace_sleep.as_mut() {
                        sleep.as_mut().await;
                    }
                }, if grace_sleep.is_some() => {
                    output_drained.notify_one();
                    break;
                }

                received = receiver.recv() => {
                    let chunk = match received {
                        Ok(chunk) => chunk,
                        Err(RecvError::Lagged(_)) => {
                            continue;
                        },
                        Err(RecvError::Closed) => {
                            output_drained.notify_one();
                            break;
                        }
                    };

                    process_chunk(
                        &mut pending,
                        &transcript,
                        &call_id,
                        &session_ref,
                        &turn_ref,
                        &mut emitted_deltas,
                        chunk,
                    ).await;
                }
            }
        }
    });
}

static BG_WAKE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Wake the session once when a backgrounded (unified-exec) process exits, so
/// the agent learns the outcome without polling the background terminal.
///
/// Reuses the mailbox + pending-work path, which is idle-gated: it only starts
/// a fresh turn when the session is idle (a truly backgrounded command). If a
/// turn is active — e.g. a short command the model is still reading — the item
/// simply drains into that turn, so this never spawns a spurious turn or
/// interrupts ongoing work.
async fn wake_session_on_background_exit(session: &Arc<Session>, command: &str, outcome: &str) {
    let comm = InterAgentCommunication {
        id: None,
        author: AgentPath::morpheus(),
        recipient: AgentPath::root(),
        other_recipients: Vec::new(),
        content: format!(
            "Background command finished — `{command}` {outcome}. Its output is in the exec \
             results; act on it if needed, otherwise continue. You were woken on exit, so do \
             not poll background commands."
        ),
        encrypted_content: None,
        internal_chat_message_metadata_passthrough: None,
        trigger_turn: true,
    };
    let sub_id = format!("bg-exit-{}", BG_WAKE_SEQ.fetch_add(1, Ordering::Relaxed));
    session.input_queue.enqueue_mailbox_communication(comm).await;
    session
        .maybe_start_turn_for_pending_work_with_sub_id(sub_id)
        .await;
}

/// Spawn a background watcher that waits for the PTY to exit and then emits a
/// single ExecCommandEnd event with the aggregated transcript.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_exit_watcher(
    process: Arc<UnifiedExecProcess>,
    session_ref: Arc<Session>,
    turn_ref: Arc<TurnContext>,
    call_id: String,
    command: Vec<String>,
    cwd: PathUri,
    process_id: i32,
    transcript: Arc<Mutex<HeadTailBuffer>>,
    started_at: Instant,
    // Set true (once) only when the initiating exec_command call reported this
    // process as still running — i.e. the model was told it is backgrounded.
    // We wake the model on exit only in that case; a command that completed
    // within its initial call already delivered its output there and must not
    // wake (waking it would spuriously start a turn and can cascade).
    wake_on_exit: Arc<AtomicBool>,
) {
    let exit_token = process.cancellation_token();
    let output_drained = process.output_drained_notify();

    tokio::spawn(async move {
        exit_token.cancelled().await;
        output_drained.notified().await;

        let duration = Instant::now().saturating_duration_since(started_at);
        // The emit_* calls consume `session_ref` and `command`; keep a handle
        // and display string for the completion wake below.
        let session_for_wake = Arc::clone(&session_ref);
        let command_display = command.join(" ");
        let outcome = if let Some(message) = process.failure_message() {
            emit_failed_exec_end_for_unified_exec(
                session_ref,
                turn_ref,
                call_id,
                command,
                cwd,
                Some(process_id.to_string()),
                transcript,
                String::new(),
                message,
                duration,
            )
            .await;
            "failed".to_string()
        } else {
            let exit_code = process.exit_code().unwrap_or(-1);
            emit_exec_end_for_unified_exec(
                session_ref,
                turn_ref,
                call_id,
                command,
                cwd,
                Some(process_id.to_string()),
                transcript,
                String::new(),
                exit_code,
                duration,
            )
            .await;
            format!("exited (code {exit_code})")
        };

        // Push a single completion wake so the agent reacts on exit instead of
        // polling the background terminal — but only for a genuinely backgrounded
        // process (the initial call already returned). Short synchronous commands
        // deliver their output through the initiating call and must not wake.
        if wake_on_exit.load(Ordering::Acquire) {
            wake_session_on_background_exit(&session_for_wake, &command_display, &outcome).await;
        }
    });
}

async fn process_chunk(
    pending: &mut Vec<u8>,
    transcript: &Arc<Mutex<HeadTailBuffer>>,
    call_id: &str,
    session_ref: &Arc<Session>,
    turn_ref: &Arc<TurnContext>,
    emitted_deltas: &mut usize,
    chunk: Vec<u8>,
) {
    pending.extend_from_slice(&chunk);
    while let Some(prefix) = split_valid_utf8_prefix(pending) {
        {
            let mut guard = transcript.lock().await;
            guard.push_chunk(prefix.to_vec());
        }

        if *emitted_deltas >= MAX_EXEC_OUTPUT_DELTAS_PER_CALL {
            continue;
        }

        let event = ExecCommandOutputDeltaEvent {
            call_id: call_id.to_string(),
            stream: ExecOutputStream::Stdout,
            chunk: prefix,
        };
        session_ref
            .send_event(turn_ref.as_ref(), EventMsg::ExecCommandOutputDelta(event))
            .await;
        *emitted_deltas += 1;
    }
}

/// Emit an ExecCommandEnd event for a unified exec session, using the transcript
/// as the primary source of aggregated_output and falling back to the provided
/// text when the transcript is empty.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_exec_end_for_unified_exec(
    session_ref: Arc<Session>,
    turn_ref: Arc<TurnContext>,
    call_id: String,
    command: Vec<String>,
    cwd: PathUri,
    process_id: Option<String>,
    transcript: Arc<Mutex<HeadTailBuffer>>,
    fallback_output: String,
    exit_code: i32,
    duration: Duration,
) {
    let aggregated_output = resolve_aggregated_output(&transcript, fallback_output).await;
    let output = ExecToolCallOutput {
        exit_code,
        stdout: StreamOutput::new(aggregated_output.clone()),
        stderr: StreamOutput::new(String::new()),
        aggregated_output: StreamOutput::new(aggregated_output),
        duration,
        timed_out: false,
    };
    let event_ctx = ToolEventCtx::new(
        session_ref.as_ref(),
        turn_ref.as_ref(),
        &call_id,
        /*turn_diff_tracker*/ None,
    );
    let emitter = ToolEmitter::unified_exec(
        &command,
        cwd,
        ExecCommandSource::UnifiedExecStartup,
        process_id,
    );
    emitter
        .emit(
            event_ctx,
            ToolEventStage::Success {
                output,
                applied_patch_delta: None,
            },
        )
        .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_failed_exec_end_for_unified_exec(
    session_ref: Arc<Session>,
    turn_ref: Arc<TurnContext>,
    call_id: String,
    command: Vec<String>,
    cwd: PathUri,
    process_id: Option<String>,
    transcript: Arc<Mutex<HeadTailBuffer>>,
    fallback_output: String,
    message: String,
    duration: Duration,
) {
    let stdout = if fallback_output.is_empty() {
        resolve_aggregated_output(&transcript, fallback_output).await
    } else {
        fallback_output
    };
    let aggregated_output = if stdout.is_empty() {
        message.clone()
    } else {
        format!("{stdout}\n{message}")
    };
    let output = ExecToolCallOutput {
        exit_code: -1,
        stdout: StreamOutput::new(stdout),
        stderr: StreamOutput::new(message),
        aggregated_output: StreamOutput::new(aggregated_output),
        duration,
        timed_out: false,
    };
    let event_ctx = ToolEventCtx::new(
        session_ref.as_ref(),
        turn_ref.as_ref(),
        &call_id,
        /*turn_diff_tracker*/ None,
    );
    let emitter = ToolEmitter::unified_exec(
        &command,
        cwd,
        ExecCommandSource::UnifiedExecStartup,
        process_id,
    );
    emitter
        .emit(
            event_ctx,
            ToolEventStage::Failure(ToolEventFailure::Output(output)),
        )
        .await;
}

fn split_valid_utf8_prefix(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    split_valid_utf8_prefix_with_max(buffer, UNIFIED_EXEC_OUTPUT_DELTA_MAX_BYTES)
}

fn split_valid_utf8_prefix_with_max(buffer: &mut Vec<u8>, max_bytes: usize) -> Option<Vec<u8>> {
    if buffer.is_empty() {
        return None;
    }

    let max_len = buffer.len().min(max_bytes);
    let mut split = max_len;
    while split > 0 {
        if std::str::from_utf8(&buffer[..split]).is_ok() {
            let prefix = buffer[..split].to_vec();
            buffer.drain(..split);
            return Some(prefix);
        }

        if max_len - split > 4 {
            break;
        }
        split -= 1;
    }

    // If no valid UTF-8 prefix was found, emit the first byte so the stream
    // keeps making progress and the transcript reflects all bytes.
    let byte = buffer.drain(..1).collect();
    Some(byte)
}

async fn resolve_aggregated_output(
    transcript: &Arc<Mutex<HeadTailBuffer>>,
    fallback: String,
) -> String {
    let guard = transcript.lock().await;
    if guard.retained_bytes() == 0 {
        return fallback;
    }

    String::from_utf8_lossy(&guard.to_bytes()).to_string()
}

#[cfg(test)]
#[path = "async_watcher_tests.rs"]
mod tests;
