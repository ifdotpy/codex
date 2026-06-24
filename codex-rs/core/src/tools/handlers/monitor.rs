// monitor.rs — stream events from a long-running background command.
//
// The `monitor` tool starts a background process and turns each of its stdout
// lines into an *event* that is injected back into the conversation as a new
// turn — without the model having to poll. This is the Codex analogue of the
// "Monitor" primitive in other agent harnesses.
//
// The wake mechanism reuses Codex's existing mailbox + pending-work machinery:
// each event is enqueued as an `InterAgentCommunication` with `trigger_turn`
// set, then `maybe_start_turn_for_pending_work_with_sub_id` starts a turn when
// the session is idle (or the item is drained into the next turn if one is
// already active). This is exactly what `session::handlers::inter_agent_communication`
// does; we inline the two `pub(crate)` steps because that handler lives in a
// private module.

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_protocol::AgentPath;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MonitorEventEvent;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command;

const MONITOR_TOOL_NAME: &str = "monitor";

/// Global counter so each monitor (and each of its events) gets a unique id,
/// without relying on `Math.random`/time which are awkward in this codebase.
static MONITOR_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct MonitorHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    /// Shell command/script to run in the background.
    command: String,
    /// Short human-readable description, surfaced with every event.
    description: String,
}

fn create_monitor_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "command".to_string(),
            JsonSchema::string(Some(
                "Shell command or script to run in the background. Each stdout line becomes \
                 an event delivered to you as a new message. Exit ends the watch. Make the \
                 filter selective (e.g. `tail -f log | grep --line-buffered ERROR`) — every \
                 line is a separate notification."
                    .to_string(),
            )),
        ),
        (
            "description".to_string(),
            JsonSchema::string(Some(
                "Short description of what is being monitored (shown with each event)."
                    .to_string(),
            )),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: MONITOR_TOOL_NAME.to_string(),
        description:
            "Start a background monitor that streams events from a long-running command. Each \
             stdout line is delivered to you as a new message (an event) as it arrives — you do \
             not poll. Use for tailing logs, watching files, or polling loops. Returns \
             immediately; the watch keeps running across turns until the command exits."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["command".to_string(), "description".to_string()]),
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}

/// Enqueue one monitor event and wake the session if it is idle.
async fn deliver_event(session: &Arc<Session>, description: &str, line: String) {
    let sub_id = format!("monitor-{}", MONITOR_SEQ.fetch_add(1, Ordering::Relaxed));

    // 1) Visible, distinct notice in the UI so the user sees what fired.
    session
        .send_event_raw(Event {
            id: sub_id.clone(),
            msg: EventMsg::MonitorEvent(MonitorEventEvent {
                description: description.to_string(),
                line: line.clone(),
            }),
        })
        .await;

    // 2) Feed the event to the model and wake the session. This is the inlined
    // body of `session::handlers::inter_agent_communication` (that handler
    // lives in a private module): queue the event as a mailbox item, then
    // start a turn for it when the session is idle (or let an active turn drain
    // it). The resulting `RawResponseItem` is not rendered by the TUI, so there
    // is no duplicate of the notice above.
    let comm = InterAgentCommunication {
        id: None,
        author: AgentPath::morpheus(),
        recipient: AgentPath::root(),
        other_recipients: Vec::new(),
        content: format!("[monitor: {description}] {line}"),
        encrypted_content: None,
        internal_chat_message_metadata_passthrough: None,
        trigger_turn: true,
    };
    session.input_queue.enqueue_mailbox_communication(comm).await;
    session
        .maybe_start_turn_for_pending_work_with_sub_id(sub_id)
        .await;
}

impl ToolExecutor<ToolInvocation> for MonitorHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(MONITOR_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_monitor_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            let ToolInvocation {
                session, payload, ..
            } = invocation;
            let ToolPayload::Function { arguments } = payload else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "{MONITOR_TOOL_NAME} handler received unsupported payload"
                )));
            };
            let args: MonitorArgs = parse_arguments(&arguments)?;

            let monitor_id = MONITOR_SEQ.fetch_add(1, Ordering::Relaxed);
            let description = args.description.clone();
            let command = args.command.clone();
            let session_for_task = Arc::clone(&session);
            // The closure consumes its copy; keep `description` for the reply.
            let description_for_task = description.clone();

            // Spawn the watcher. The tool call itself returns immediately; the
            // process keeps running across turns, delivering events as they
            // arrive, and a final event when it exits.
            tokio::spawn(async move {
                let mut child = match Command::new("bash")
                    .arg("-lc")
                    .arg(&command)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()
                {
                    Ok(child) => child,
                    Err(err) => {
                        deliver_event(
                            &session_for_task,
                            &description_for_task,
                            format!("failed to start: {err}"),
                        )
                        .await;
                        return;
                    }
                };

                if let Some(stdout) = child.stdout.take() {
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if line.trim().is_empty() {
                            continue;
                        }
                        deliver_event(&session_for_task, &description_for_task, line).await;
                    }
                }

                let exit = match child.wait().await {
                    Ok(status) => format!("exited ({status})"),
                    Err(err) => format!("wait error: {err}"),
                };
                deliver_event(&session_for_task, &description_for_task, format!("monitor {exit}")).await;
            });

            Ok(boxed_tool_output(FunctionToolOutput::from_text(
                format!(
                    "Started background monitor #{monitor_id}: {description}. \
                     Events will arrive as messages tagged `[monitor: {description}]` \
                     as the command produces output; a final event is delivered when it exits."
                ),
                /*success*/ Some(true),
            )))
        })
    }
}

impl CoreToolRuntime for MonitorHandler {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_spec_is_function_with_required_args() {
        let ToolSpec::Function(tool) = MonitorHandler.spec() else {
            panic!("expected a function tool spec");
        };
        assert_eq!(tool.name, MONITOR_TOOL_NAME);
        let params = serde_json::to_value(&tool.parameters).expect("schema serializes");
        let required = params
            .get("required")
            .and_then(|r| r.as_array())
            .expect("required array present");
        assert!(required.iter().any(|v| v == "command"));
        assert!(required.iter().any(|v| v == "description"));
    }
}
