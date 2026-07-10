use anyhow::Result;
use codex_features::Feature;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use core_test_support::TestTargetOs;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::local_selections;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::turn_permission_fields;
use core_test_support::test_target_os;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::sync::oneshot;

const NOTIFICATION_START: &str = "<background_terminal_notification>";

fn notification_texts(body: &serde_json::Value) -> Vec<String> {
    body.get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("type").and_then(serde_json::Value::as_str) == Some("message"))
        .filter(|item| item.get("role").and_then(serde_json::Value::as_str) == Some("user"))
        .filter_map(|item| item.get("content").and_then(serde_json::Value::as_array))
        .flatten()
        .filter(|span| span.get("type").and_then(serde_json::Value::as_str) == Some("input_text"))
        .filter_map(|span| span.get("text").and_then(serde_json::Value::as_str))
        .filter(|text| text.starts_with(NOTIFICATION_START))
        .map(str::to_owned)
        .collect()
}

fn function_call_output<'a>(body: &'a serde_json::Value, call_id: &str) -> Option<&'a str> {
    body.get("input")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find(|item| {
            item.get("type").and_then(serde_json::Value::as_str) == Some("function_call_output")
                && item.get("call_id").and_then(serde_json::Value::as_str) == Some(call_id)
        })?
        .get("output")
        .and_then(serde_json::Value::as_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monitor_wakes_model_for_output_and_exit() -> Result<()> {
    let server = start_mock_server().await;
    let command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => {
            "printf 'MONITOR_EARLY\\n'; sleep 0.7; printf 'MONITOR_LATE\\n' >&2; sleep 1.5"
        }
        TestTargetOs::Windows => {
            "Write-Output MONITOR_EARLY; Start-Sleep -Milliseconds 2700; [Console]::Error.WriteLine('MONITOR_LATE'); Start-Sleep -Milliseconds 1500"
        }
    };
    let arguments = serde_json::to_string(&json!({
        "command": command,
        "description": "late output",
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("response-1"),
                ev_function_call("monitor-call", "monitor", &arguments),
                ev_completed("response-1"),
            ]),
            sse(vec![
                ev_response_created("response-2"),
                ev_assistant_message("message-2", "waiting"),
                ev_completed("response-2"),
            ]),
            sse(vec![
                ev_response_created("response-3"),
                ev_assistant_message("message-3", "output received"),
                ev_completed("response-3"),
            ]),
            sse(vec![
                ev_response_created("response-4"),
                ev_assistant_message("message-4", "monitor finished"),
                ev_completed("response-4"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex()
        .with_session_source(SessionSource::Cli)
        .with_config(|config| {
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec enables");
            config
                .features
                .enable(Feature::MonitorTool)
                .expect("monitor enables");
        });
    let test = builder.build_with_auto_env(&server).await?;

    test.submit_turn("watch late output").await?;
    let observed_all_requests = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if responses.requests().len() == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        observed_all_requests.is_ok(),
        "expected four model requests, got {}",
        responses.requests().len()
    );

    let requests = responses.requests();
    assert_eq!(requests.len(), 4);
    let initial_output_item = requests[1].function_call_output("monitor-call");
    let initial_output = initial_output_item
        .get("output")
        .and_then(serde_json::Value::as_str)
        .expect("monitor tool output");
    let output_notifications = requests[2]
        .message_input_texts("user")
        .into_iter()
        .filter(|text| text.starts_with(NOTIFICATION_START))
        .collect::<Vec<_>>();
    let exit_notifications = requests[3]
        .message_input_texts("user")
        .into_iter()
        .filter(|text| text.starts_with(NOTIFICATION_START))
        .collect::<Vec<_>>();

    assert!(initial_output.contains("MONITOR_EARLY"));
    assert_eq!(output_notifications.len(), 1);
    assert!(output_notifications[0].contains("MONITOR_LATE"));
    assert!(!output_notifications[0].contains("MONITOR_EARLY"));
    assert!(output_notifications[0].contains(r#""type":"output_available""#));
    assert_eq!(exit_notifications.len(), 2);
    assert!(exit_notifications[1].contains(r#""type":"exited""#));
    assert!(exit_notifications[1].contains(r#""exit_code":0"#));
    assert_eq!(responses.requests().len(), 4);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_monitor_lets_explicit_write_stdin_own_output_and_exit() -> Result<()> {
    let server = start_mock_server().await;
    let command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => "sleep 0.7; printf 'WAITED\\n'",
        TestTargetOs::Windows => "Start-Sleep -Milliseconds 2700; Write-Output WAITED",
    };
    let monitor_arguments = serde_json::to_string(&json!({
        "command": command,
        "description": "explicit wait",
        "timeout_ms": 1,
        "persistent": true,
    }))?;
    let wait_arguments = serde_json::to_string(&json!({
        "session_id": 1000,
        "chars": "",
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("response-1"),
                ev_function_call("monitor-call", "monitor", &monitor_arguments),
                ev_completed("response-1"),
            ]),
            sse(vec![
                ev_response_created("response-2"),
                ev_function_call("wait-call", "write_stdin", &wait_arguments),
                ev_completed("response-2"),
            ]),
            sse(vec![
                ev_response_created("response-3"),
                ev_assistant_message("message-3", "wait completed"),
                ev_completed("response-3"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex()
        .with_session_source(SessionSource::Cli)
        .with_config(|config| {
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec enables");
            config
                .features
                .enable(Feature::MonitorTool)
                .expect("monitor enables");
        });
    let test = builder.build_with_auto_env(&server).await?;

    test.submit_turn("wait for monitor completion").await?;

    let requests = responses.requests();
    assert_eq!(requests.len(), 3);
    let wait_output_item = requests[2].function_call_output("wait-call");
    let wait_output = wait_output_item
        .get("output")
        .and_then(serde_json::Value::as_str)
        .expect("write_stdin output");
    assert!(wait_output.contains("WAITED"));
    wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::ExecCommandEnd(event) if event.call_id == "monitor-call"),
    )
    .await;
    assert_eq!(responses.requests().len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monitor_timeout_terminates_process_and_wakes_model() -> Result<()> {
    let server = start_mock_server().await;
    let command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => "sleep 5",
        TestTargetOs::Windows => "Start-Sleep -Seconds 7",
    };
    let arguments = serde_json::to_string(&json!({
        "command": command,
        "description": "timeout",
        "timeout_ms": match test_target_os() {
            TestTargetOs::Linux | TestTargetOs::MacOs => 700,
            TestTargetOs::Windows => 2700,
        },
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("response-1"),
                ev_function_call("monitor-call", "monitor", &arguments),
                ev_completed("response-1"),
            ]),
            sse(vec![
                ev_response_created("response-2"),
                ev_assistant_message("message-2", "waiting"),
                ev_completed("response-2"),
            ]),
            sse(vec![
                ev_response_created("response-3"),
                ev_assistant_message("message-3", "timeout received"),
                ev_completed("response-3"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex()
        .with_session_source(SessionSource::Cli)
        .with_config(|config| {
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec enables");
            config
                .features
                .enable(Feature::MonitorTool)
                .expect("monitor enables");
        });
    let test = builder.build_with_auto_env(&server).await?;

    test.submit_turn("watch timeout").await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if responses.requests().len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timeout notification request");

    let requests = responses.requests();
    assert_eq!(requests.len(), 3);
    let notifications = requests[2]
        .message_input_texts("user")
        .into_iter()
        .filter(|text| text.starts_with(NOTIFICATION_START))
        .collect::<Vec<_>>();
    assert_eq!(notifications.len(), 1);
    assert!(notifications[0].contains(r#""type":"failed""#));
    let expected_timeout = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => 700,
        TestTargetOs::Windows => 2700,
    };
    assert!(notifications[0].contains(&format!("monitor timed out after {expected_timeout} ms")));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_lived_monitor_returns_inline_without_wake() -> Result<()> {
    let server = start_mock_server().await;
    let command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => "printf 'SHORT_LIVED\\n'",
        TestTargetOs::Windows => "Write-Output SHORT_LIVED",
    };
    let arguments = serde_json::to_string(&json!({
        "command": command,
        "description": "short lived",
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("response-1"),
                ev_function_call("monitor-call", "monitor", &arguments),
                ev_completed("response-1"),
            ]),
            sse(vec![
                ev_response_created("response-2"),
                ev_assistant_message("message-2", "completed inline"),
                ev_completed("response-2"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex()
        .with_session_source(SessionSource::Cli)
        .with_config(|config| {
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec enables");
            config
                .features
                .enable(Feature::MonitorTool)
                .expect("monitor enables");
        });
    let test = builder.build_with_auto_env(&server).await?;

    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, test.cwd.path());
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "run short monitor".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: codex_protocol::protocol::ThreadSettingsOverrides {
                environments: Some(local_selections(test.config.cwd.clone())),
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: Some(sandbox_policy),
                permission_profile,
                ..Default::default()
            },
        })
        .await?;
    wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::ExecCommandEnd(event) if event.call_id == "monitor-call"),
    )
    .await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    let output_item = requests[1].function_call_output("monitor-call");
    let output = output_item
        .get("output")
        .and_then(serde_json::Value::as_str)
        .expect("monitor tool output");
    assert!(output.contains("SHORT_LIVED"));
    assert_eq!(responses.requests().len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monitor_caps_notifications_without_starting_a_concurrent_turn() -> Result<()> {
    let command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => {
            "sleep 2; batch=0; while [ $batch -lt 64 ]; do line=0; output=''; while [ $line -lt 20 ]; do output=\"${output}line-${batch}-${line}\\n\"; line=$((line+1)); done; printf '%b' \"$output\"; sleep 0.02; batch=$((batch+1)); done; sleep 30"
        }
        TestTargetOs::Windows => {
            "Start-Sleep -Milliseconds 3000; 0..63 | ForEach-Object { $batch = $_; $lines = 0..19 | ForEach-Object { \"line-$batch-$_\" }; [Console]::Out.Write(($lines -join \"`n\") + \"`n\"); Start-Sleep -Milliseconds 20 }; Start-Sleep -Seconds 30"
        }
    };
    let arguments = serde_json::to_string(&json!({
        "command": command,
        "description": "notification cap",
    }))?;
    let (complete_active_turn_tx, complete_active_turn_rx) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("response-1"),
                ev_function_call("monitor-call", "monitor", &arguments),
                ev_completed("response-1"),
            ]),
        }],
        vec![
            StreamingSseChunk {
                gate: None,
                body: sse(vec![
                    ev_response_created("response-2"),
                    ev_assistant_message("message-2", "waiting"),
                ]),
            },
            StreamingSseChunk {
                gate: Some(complete_active_turn_rx),
                body: sse(vec![ev_completed("response-2")]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("response-3"),
                ev_assistant_message("message-3", "cap received"),
                ev_completed("response-3"),
            ]),
        }],
    ])
    .await;

    let mut builder = test_codex()
        .with_session_source(SessionSource::Cli)
        .with_config(|config| {
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec enables");
            config
                .features
                .enable(Feature::MonitorTool)
                .expect("monitor enables");
        });
    let test = builder.build_with_streaming_server(&server).await?;

    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, test.cwd.path());
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "cap monitor notifications".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: codex_protocol::protocol::ThreadSettingsOverrides {
                environments: Some(local_selections(test.config.cwd.clone())),
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: Some(sandbox_policy),
                permission_profile,
                ..Default::default()
            },
        })
        .await?;
    server.wait_for_request_count(2).await;
    let requests = server.requests().await;
    let tool_result_request: serde_json::Value =
        serde_json::from_slice(&requests[1]).expect("parse tool result request");
    let tool_output = function_call_output(&tool_result_request, "monitor-call")
        .expect("monitor function output");
    assert!(
        tool_output.contains("Process running with session ID 1000"),
        "{tool_output}"
    );
    wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::ExecCommandEnd(event) if event.call_id == "monitor-call"),
    )
    .await;
    assert_eq!(server.requests().await.len(), 2);

    complete_active_turn_tx
        .send(())
        .expect("release active model turn");
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        server.wait_for_request_count(3),
    )
    .await
    .expect("monitor follow-up request");
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let requests = server.requests().await;
    assert_eq!(requests.len(), 3);
    let follow_up: serde_json::Value =
        serde_json::from_slice(&requests[2]).expect("parse follow-up request");
    let notifications = notification_texts(&follow_up);
    assert_eq!(notifications.len(), 64);
    assert!(
        notifications
            .last()
            .is_some_and(|text| text.contains("monitor stopped after 64 output notifications"))
    );

    server.shutdown().await;
    Ok(())
}
