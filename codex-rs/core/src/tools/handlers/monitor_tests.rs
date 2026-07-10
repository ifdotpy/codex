use pretty_assertions::assert_eq;

use super::*;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::tools::context::ToolCallSource;
use crate::turn_diff_tracker::TurnDiffTracker;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn invocation(arguments: Value) -> ToolInvocation {
    let (session, turn) = make_session_and_context().await;
    let turn = Arc::new(turn);
    ToolInvocation {
        session: session.into(),
        step_context: StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
        call_id: "monitor-call".to_string(),
        tool_name: ToolName::plain(MONITOR_TOOL_NAME),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    }
}

#[test]
fn monitor_exec_arguments_use_unified_exec_shape() {
    let args = MonitorArgs {
        command: "tail -f app.log".to_string(),
        description: "app errors".to_string(),
        workdir: Some("/tmp/project".to_string()),
        environment_id: Some("primary".to_string()),
        max_output_tokens: Some(123),
        timeout_ms: None,
        persistent: false,
    };

    let value: Value =
        serde_json::from_str(&exec_arguments_for_monitor(args).expect("arguments")).expect("json");

    assert_eq!(
        value,
        serde_json::json!({
            "cmd": "tail -f app.log",
            "environment_id": "primary",
            "max_output_tokens": 123,
            "tty": false,
            "workdir": "/tmp/project",
            "yield_time_ms": DEFAULT_MONITOR_YIELD_TIME_MS,
        })
    );
}

#[test]
fn monitor_timeout_is_bounded_and_persistent_disables_it() {
    let mut args = MonitorArgs {
        command: "tail -f app.log".to_string(),
        description: "app errors".to_string(),
        workdir: None,
        environment_id: None,
        max_output_tokens: None,
        timeout_ms: Some(MAX_MONITOR_TIMEOUT_MS),
        persistent: false,
    };

    assert_eq!(
        monitor_config(&args)
            .expect("maximum timeout is valid")
            .timeout,
        Some(std::time::Duration::from_millis(MAX_MONITOR_TIMEOUT_MS))
    );
    args.persistent = true;
    assert_eq!(
        monitor_config(&args)
            .expect("persistent monitor is valid")
            .timeout,
        None
    );
    args.timeout_ms = Some(MAX_MONITOR_TIMEOUT_MS + 1);
    assert!(monitor_config(&args).is_err());
}

#[tokio::test]
async fn monitor_uses_bash_hook_contract() {
    let invocation = invocation(serde_json::json!({
        "command": "tail -f app.log",
        "description": "errors",
    }))
    .await;
    let handler = MonitorHandler::default();

    assert_eq!(
        handler.pre_tool_use_payload(&invocation),
        Some(PreToolUsePayload {
            tool_name: HookToolName::bash(),
            tool_input: serde_json::json!({ "command": "tail -f app.log" }),
        })
    );
}

#[tokio::test]
async fn monitor_applies_bash_hook_command_rewrites() {
    let invocation = invocation(serde_json::json!({
        "command": "tail -f app.log",
        "description": "errors",
    }))
    .await;
    let handler = MonitorHandler::default();

    let rewritten = handler
        .with_updated_hook_input(
            invocation,
            serde_json::json!({ "command": "tail -n 10 app.log" }),
        )
        .expect("rewrite succeeds");
    let ToolPayload::Function { arguments } = rewritten.payload else {
        panic!("expected function payload");
    };

    assert_eq!(
        serde_json::from_str::<Value>(&arguments).expect("valid json"),
        serde_json::json!({
            "command": "tail -n 10 app.log",
            "description": "errors",
        })
    );
}
