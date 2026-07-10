use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::Sleep;

use super::UnifiedExecContext;
use super::async_watcher::TRAILING_OUTPUT_GRACE;
use super::process::UnifiedExecProcess;
use crate::context::BackgroundTerminalNotification;
use crate::context::ContextualUserFragment;
use crate::session::session::Session;
use codex_utils_string::take_bytes_at_char_boundary;

const MONITOR_FLUSH_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(800);
const MONITOR_MAX_BATCH_LINES: usize = 20;
pub(super) const MONITOR_MAX_LINE_BYTES: usize = 512;
const MONITOR_MAX_PENDING_BYTES: usize = 2_048;
const MONITOR_MAX_NOTIFICATIONS: usize = 64;

pub(crate) struct MonitorOutputHandle {
    armed_tx: watch::Sender<bool>,
    completed_rx: watch::Receiver<bool>,
}

impl MonitorOutputHandle {
    pub(crate) fn arm(&self) {
        self.armed_tx.send_replace(true);
    }

    pub(crate) fn completion_receiver(&self) -> watch::Receiver<bool> {
        self.completed_rx.clone()
    }
}

struct MonitorCompletionGuard {
    completed_tx: watch::Sender<bool>,
}

impl Drop for MonitorCompletionGuard {
    fn drop(&mut self) {
        self.completed_tx.send_replace(true);
    }
}

pub(crate) fn start_monitoring_output(
    process: Arc<UnifiedExecProcess>,
    context: &UnifiedExecContext,
    process_id: i32,
    description: String,
    explicit_waiters: Arc<AtomicUsize>,
) -> MonitorOutputHandle {
    let mut receiver = process.output_receiver();
    let exit_token = process.cancellation_token();
    let session = Arc::clone(&context.session);
    let (armed_tx, mut armed_rx) = watch::channel(false);
    let (completed_tx, completed_rx) = watch::channel(false);

    tokio::spawn(async move {
        use tokio::sync::broadcast::error::RecvError;
        use tokio::sync::broadcast::error::TryRecvError;

        let _completion_guard = MonitorCompletionGuard { completed_tx };
        loop {
            if *armed_rx.borrow() {
                break;
            }
            tokio::select! {
                _ = exit_token.cancelled() => return,
                changed = armed_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }

        loop {
            match receiver.try_recv() {
                Ok(_) | Err(TryRecvError::Lagged(_)) => {}
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => return,
            }
        }

        let mut pending = Vec::<u8>::new();
        let mut lines = Vec::<String>::new();
        let mut omitted_bytes = 0usize;
        let mut notifications = 0usize;
        let mut grace_sleep: Option<Pin<Box<Sleep>>> = None;

        while notifications < MONITOR_MAX_NOTIFICATIONS {
            tokio::select! {
                _ = exit_token.cancelled(), if grace_sleep.is_none() => {
                    let deadline = Instant::now() + TRAILING_OUTPUT_GRACE;
                    grace_sleep.replace(Box::pin(tokio::time::sleep_until(deadline)));
                }
                _ = async {
                    if let Some(sleep) = grace_sleep.as_mut() {
                        sleep.as_mut().await;
                    }
                }, if grace_sleep.is_some() => break,
                received = receiver.recv() => {
                    match received {
                        Ok(chunk) => {
                            if explicit_waiters.load(Ordering::Acquire) > 0 {
                                pending.clear();
                                lines.clear();
                                omitted_bytes = 0;
                                continue;
                            }
                            push_monitor_chunk(
                                &mut pending,
                                &mut lines,
                                &mut omitted_bytes,
                                &chunk,
                            );
                            while let Some(batch) = take_ready_monitor_batch(&mut lines) {
                                deliver_monitor_batch(
                                    &session,
                                    process_id,
                                    &description,
                                    batch,
                                    std::mem::take(&mut omitted_bytes),
                                ).await;
                                notifications += 1;
                                if notifications == MONITOR_MAX_NOTIFICATIONS {
                                    break;
                                }
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            lines.push(format!("[monitor skipped {skipped} output chunk(s)]"));
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                _ = tokio::time::sleep(MONITOR_FLUSH_DEBOUNCE), if !lines.is_empty() => {
                    if explicit_waiters.load(Ordering::Acquire) > 0 {
                        pending.clear();
                        lines.clear();
                        omitted_bytes = 0;
                    } else {
                        deliver_monitor_batch(
                            &session,
                            process_id,
                            &description,
                            std::mem::take(&mut lines),
                            std::mem::take(&mut omitted_bytes),
                        ).await;
                        notifications += 1;
                    }
                }
            }
        }

        while pending
            .last()
            .is_some_and(|byte| matches!(byte, b'\r' | b'\n'))
        {
            pending.pop();
        }
        if !pending.is_empty() && notifications < MONITOR_MAX_NOTIFICATIONS {
            let (line, omitted) = truncate_monitor_line(&pending);
            lines.push(line);
            omitted_bytes = omitted_bytes.saturating_add(omitted);
        }
        if !lines.is_empty() && notifications < MONITOR_MAX_NOTIFICATIONS {
            deliver_monitor_batch(&session, process_id, &description, lines, omitted_bytes).await;
        }
        if notifications >= MONITOR_MAX_NOTIFICATIONS {
            process.fail_and_terminate(format!(
                "monitor stopped after {MONITOR_MAX_NOTIFICATIONS} output notifications"
            ));
        }
    });

    MonitorOutputHandle {
        armed_tx,
        completed_rx,
    }
}

pub(crate) fn start_monitor_timeout(
    process: Arc<UnifiedExecProcess>,
    timeout: std::time::Duration,
) {
    let exit_token = process.cancellation_token();
    tokio::spawn(async move {
        tokio::select! {
            _ = exit_token.cancelled() => {}
            _ = tokio::time::sleep(timeout) => {
                process.fail_and_terminate(format!(
                    "monitor timed out after {} ms",
                    timeout.as_millis()
                ));
            }
        }
    });
}

pub(super) fn push_monitor_chunk(
    pending: &mut Vec<u8>,
    lines: &mut Vec<String>,
    omitted_bytes: &mut usize,
    chunk: &[u8],
) {
    pending.extend_from_slice(chunk);

    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
        let mut line = pending.drain(..=newline).collect::<Vec<_>>();
        while line
            .last()
            .is_some_and(|byte| matches!(byte, b'\r' | b'\n'))
        {
            line.pop();
        }
        if !String::from_utf8_lossy(&line).trim().is_empty() {
            let (line, omitted) = truncate_monitor_line(&line);
            lines.push(line);
            *omitted_bytes = omitted_bytes.saturating_add(omitted);
        }
    }

    if pending.len() > MONITOR_MAX_PENDING_BYTES {
        let (line, omitted) = truncate_monitor_line(pending);
        lines.push(format!("{line} [unterminated line truncated]"));
        *omitted_bytes = omitted_bytes.saturating_add(omitted);
        pending.clear();
    }
}

fn truncate_monitor_line(line: &[u8]) -> (String, usize) {
    let text = String::from_utf8_lossy(line);
    let retained = take_bytes_at_char_boundary(&text, MONITOR_MAX_LINE_BYTES);
    (
        retained.to_string(),
        line.len().saturating_sub(retained.len()),
    )
}

fn take_ready_monitor_batch(lines: &mut Vec<String>) -> Option<Vec<String>> {
    (lines.len() >= MONITOR_MAX_BATCH_LINES)
        .then(|| lines.drain(..MONITOR_MAX_BATCH_LINES).collect())
}

async fn deliver_monitor_batch(
    session: &Arc<Session>,
    process_id: i32,
    description: &str,
    lines: Vec<String>,
    omitted_bytes: usize,
) {
    if lines.is_empty() {
        return;
    }
    enqueue_background_notification(
        session,
        BackgroundTerminalNotification::output(
            process_id,
            description,
            &lines.join("\n"),
            omitted_bytes,
        ),
    )
    .await;
}

pub(super) async fn enqueue_background_notification(
    session: &Arc<Session>,
    notification: BackgroundTerminalNotification,
) {
    session
        .input_queue
        .enqueue_background_context(
            &session.active_turn,
            ContextualUserFragment::into(notification),
        )
        .await;
    session.maybe_start_turn_for_pending_work().await;
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
