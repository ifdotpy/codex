use anyhow::Result;
use codex_features::Feature;
use codex_protocol::config_types::MultiAgentMode;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::openai_models::ReasoningEffortPreset;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MULTI_AGENT_MODE_OPEN_TAG;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;

const NO_SPAWN_TEXT: &str = "Do not spawn sub-agents unless the user explicitly asks for sub-agents, delegation, or parallel agent work.";
const NO_MODE_TEXT: &str = "Multi-agent delegation mode instructions are inactive.";
const PROACTIVE_TEXT: &str = "Proactive multi-agent delegation is active.";

fn developer_texts(input: &[Value]) -> Vec<&str> {
    input
        .iter()
        .filter(|item| item.get("role").and_then(Value::as_str) == Some("developer"))
        .filter_map(|item| item.get("content")?.as_array())
        .flatten()
        .filter_map(|content| content.get("text")?.as_str())
        .collect()
}

fn count_containing(texts: &[&str], target: &str) -> usize {
    texts.iter().filter(|text| text.contains(target)).count()
}

async fn submit_turn(
    codex: &codex_core::CodexThread,
    prompt: &str,
    mode: Option<MultiAgentMode>,
) -> Result<()> {
    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: prompt.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: ThreadSettingsOverrides {
                multi_agent_mode: mode,
                ..Default::default()
            },
        })
        .await?;
    wait_for_event(codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_mode_is_sticky_and_emits_only_on_change() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=5)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
        })
        .build(&server)
        .await?;

    submit_turn(&test.codex, "turn one", /*mode*/ None).await?;
    assert_eq!(
        test.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::ExplicitRequestOnly
    );
    submit_turn(&test.codex, "turn two", Some(MultiAgentMode::Proactive)).await?;
    submit_turn(&test.codex, "turn three", /*mode*/ None).await?;
    submit_turn(&test.codex, "turn four", Some(MultiAgentMode::None)).await?;
    submit_turn(&test.codex, "turn five", /*mode*/ None).await?;

    assert_eq!(
        test.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::None
    );

    let requests = responses.requests();
    let inputs = requests
        .iter()
        .map(core_test_support::responses::ResponsesRequest::input)
        .collect::<Vec<_>>();
    let first = developer_texts(&inputs[0]);
    let second = developer_texts(&inputs[1]);
    let third = developer_texts(&inputs[2]);
    let fourth = developer_texts(&inputs[3]);
    let fifth = developer_texts(&inputs[4]);

    assert_eq!(
        (
            count_containing(&first, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&first, NO_SPAWN_TEXT),
            count_containing(&first, PROACTIVE_TEXT),
        ),
        (1, 1, 0)
    );
    assert_eq!(
        (
            count_containing(&second, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&second, NO_SPAWN_TEXT),
            count_containing(&second, PROACTIVE_TEXT),
        ),
        (2, 1, 1)
    );
    assert_eq!(
        (
            count_containing(&third, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&third, NO_SPAWN_TEXT),
            count_containing(&third, PROACTIVE_TEXT),
        ),
        (2, 1, 1)
    );
    assert_eq!(
        (
            count_containing(&fourth, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&fourth, NO_SPAWN_TEXT),
            count_containing(&fourth, PROACTIVE_TEXT),
            count_containing(&fourth, NO_MODE_TEXT),
        ),
        (3, 1, 1, 1)
    );
    assert_eq!(
        (
            count_containing(&fifth, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&fifth, NO_SPAWN_TEXT),
            count_containing(&fifth, PROACTIVE_TEXT),
            count_containing(&fifth, NO_MODE_TEXT),
        ),
        (3, 1, 1, 1)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ultra_reasoning_uses_proactive_mode_without_changing_legacy_selection() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let test = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::MultiAgentMode)
                .expect("test config should allow feature update");
            config.model_reasoning_effort = Some(ReasoningEffort::Ultra);
        })
        .build(&server)
        .await?;

    submit_turn(&test.codex, "hello", /*mode*/ None).await?;

    assert_eq!(
        test.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::ExplicitRequestOnly
    );
    let request = response.single_request();
    assert_eq!(
        request.body_json()["reasoning"]["effort"].as_str(),
        Some("max")
    );
    let developer_texts = request.message_input_texts("developer");
    assert_eq!(
        (
            developer_texts
                .iter()
                .filter(|text| text.contains(PROACTIVE_TEXT))
                .count(),
            developer_texts
                .iter()
                .filter(|text| text.contains(NO_SPAWN_TEXT))
                .count(),
        ),
        (1, 0)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ultra_reasoning_uses_max_without_multi_agent_mode_in_v1() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let test = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentMode)
                .expect("test config should allow feature update");
            config.model_reasoning_effort = Some(ReasoningEffort::Ultra);
        })
        .build(&server)
        .await?;

    submit_turn(&test.codex, "hello", /*mode*/ None).await?;

    let request = response.single_request();
    assert_eq!(
        request.body_json()["reasoning"]["effort"].as_str(),
        Some("max")
    );
    let developer_texts = request.message_input_texts("developer");
    assert_eq!(
        developer_texts
            .iter()
            .filter(|text| text.contains(MULTI_AGENT_MODE_OPEN_TAG))
            .count(),
        0
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ultra_reasoning_is_rejected_when_feature_is_disabled() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let test = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .build(&server)
        .await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: ThreadSettingsOverrides {
                effort: Some(Some(ReasoningEffort::Ultra)),
                ..Default::default()
            },
        })
        .await?;

    let error = wait_for_event(&test.codex, |event| matches!(event, EventMsg::Error(_))).await;
    let EventMsg::Error(error) = error else {
        unreachable!();
    };
    assert!(
        error.message.contains("features.multi_agent_mode"),
        "unexpected error: {}",
        error.message
    );
    assert_ne!(
        test.codex.config_snapshot().await.reasoning_effort,
        Some(ReasoningEffort::Ultra)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ultra_reasoning_model_default_is_rejected_when_feature_is_disabled() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let test = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info.default_reasoning_level = Some(ReasoningEffort::Ultra);
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .build(&server)
        .await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: ThreadSettingsOverrides::default(),
        })
        .await?;

    let error = wait_for_event(&test.codex, |event| matches!(event, EventMsg::Error(_))).await;
    let EventMsg::Error(error) = error else {
        unreachable!();
    };
    assert!(
        error.message.contains("features.multi_agent_mode"),
        "unexpected error: {}",
        error.message
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_mode_none_omits_instructions_and_survives_resume() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=2)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let initial = test_codex()
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
        })
        .build(&server)
        .await?;
    let home = initial.home.clone();
    let rollout_path = initial
        .session_configured
        .rollout_path
        .clone()
        .expect("rollout path");

    submit_turn(&initial.codex, "before resume", Some(MultiAgentMode::None)).await?;
    assert_eq!(
        initial.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::None
    );
    drop(initial);

    let mut resume_builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
    });
    let resumed = resume_builder.resume(&server, home, rollout_path).await?;
    submit_turn(&resumed.codex, "after resume", /*mode*/ None).await?;

    assert_eq!(
        resumed.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::None
    );
    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        let input = request.input();
        let texts = developer_texts(&input);
        assert_eq!(
            (
                count_containing(&texts, MULTI_AGENT_MODE_OPEN_TAG),
                count_containing(&texts, NO_SPAWN_TEXT),
                count_containing(&texts, PROACTIVE_TEXT),
                count_containing(&texts, NO_MODE_TEXT),
            ),
            (0, 0, 0, 0)
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_mode_applies_without_usage_hint_text() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config.multi_agent_v2.root_agent_usage_hint_text = None;
        })
        .build(&server)
        .await?;

    submit_turn(&test.codex, "hello", Some(MultiAgentMode::Proactive)).await?;

    let input = responses.single_request().input();
    let texts = developer_texts(&input);
    assert_eq!(
        (
            count_containing(&texts, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&texts, PROACTIVE_TEXT),
        ),
        (1, 1)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_compares_against_previous_effective_multi_agent_mode() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=2)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let initial = test_codex()
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
        })
        .build(&server)
        .await?;
    let home = initial.home.clone();
    let rollout_path = initial
        .session_configured
        .rollout_path
        .clone()
        .expect("rollout path");

    submit_turn(
        &initial.codex,
        "before resume",
        Some(MultiAgentMode::Proactive),
    )
    .await?;
    drop(initial);

    let mut resume_builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("test config should allow feature update");
    });
    let resumed = resume_builder.resume(&server, home, rollout_path).await?;
    submit_turn(&resumed.codex, "after resume", /*mode*/ None).await?;

    assert_eq!(
        resumed.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::Proactive
    );

    let requests = responses.requests();
    let resumed_input = requests[1].input();
    let texts = developer_texts(&resumed_input);
    assert_eq!(
        (
            count_containing(&texts, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&texts, NO_SPAWN_TEXT),
            count_containing(&texts, PROACTIVE_TEXT),
        ),
        (1, 0, 1)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_resume_from_ultra_resets_legacy_mode_before_max_turn() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        (1..=2)
            .map(|index| {
                sse(vec![
                    ev_response_created(&format!("resp-{index}")),
                    ev_completed(&format!("resp-{index}")),
                ])
            })
            .collect(),
    )
    .await;
    let initial = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info.default_reasoning_level = Some(ReasoningEffort::Ultra);
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::MultiAgentMode)
                .expect("test config should allow feature update");
        })
        .build(&server)
        .await?;
    let home = initial.home.clone();
    let rollout_path = initial
        .session_configured
        .rollout_path
        .clone()
        .expect("rollout path");

    submit_turn(&initial.codex, "before resume", /*mode*/ None).await?;
    drop(initial);

    let mut resume_builder = test_codex()
        .with_model_info_override("gpt-5.4", |model_info| {
            model_info.supports_reasoning_summaries = true;
            model_info
                .supported_reasoning_levels
                .push(ReasoningEffortPreset {
                    effort: ReasoningEffort::Ultra,
                    description: "Maximum reasoning with proactive delegation".to_string(),
                });
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::MultiAgentMode)
                .expect("test config should allow feature update");
            config.model_reasoning_effort = Some(ReasoningEffort::Custom("max".to_string()));
        });
    let resumed = resume_builder.resume(&server, home, rollout_path).await?;
    submit_turn(&resumed.codex, "after resume", /*mode*/ None).await?;

    assert_eq!(
        (
            resumed.codex.config_snapshot().await.reasoning_effort,
            resumed.codex.config_snapshot().await.multi_agent_mode,
        ),
        (
            Some(ReasoningEffort::Custom("max".to_string())),
            MultiAgentMode::ExplicitRequestOnly,
        )
    );

    let requests = responses.requests();
    let resumed_input = requests[1].input();
    let texts = developer_texts(&resumed_input);
    assert_eq!(
        (
            count_containing(&texts, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&texts, NO_SPAWN_TEXT),
            count_containing(&texts, PROACTIVE_TEXT),
        ),
        (2, 1, 1)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_mode_is_retained_without_multi_agent_v2() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let responses = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let test = test_codex().build(&server).await?;

    submit_turn(&test.codex, "hello", Some(MultiAgentMode::Proactive)).await?;

    assert_eq!(
        test.codex.config_snapshot().await.multi_agent_mode,
        MultiAgentMode::Proactive
    );
    let input = responses.single_request().input();
    let texts = developer_texts(&input);
    assert_eq!(
        (
            count_containing(&texts, MULTI_AGENT_MODE_OPEN_TAG),
            count_containing(&texts, PROACTIVE_TEXT),
        ),
        (0, 0)
    );

    Ok(())
}
