use std::collections::BTreeMap;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::unified_exec::ExecCommandHandler;
use crate::tools::handlers::unified_exec::ExecCommandHandlerOptions;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;

const MONITOR_TOOL_NAME: &str = "monitor";
const DEFAULT_MONITOR_YIELD_TIME_MS: u64 = 250;
const MAX_DESCRIPTION_CHARS: usize = 80;

pub struct MonitorHandler {
    exec_handler: ExecCommandHandler,
    include_environment_id: bool,
}

impl Default for MonitorHandler {
    fn default() -> Self {
        Self::new(ExecCommandHandlerOptions {
            allow_login_shell: false,
            exec_permission_approvals_enabled: false,
            include_environment_id: false,
            include_shell_parameter: true,
        })
    }
}

impl MonitorHandler {
    pub(crate) fn new(options: ExecCommandHandlerOptions) -> Self {
        Self {
            exec_handler: ExecCommandHandler::new(options),
            include_environment_id: options.include_environment_id,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    /// Shell command/script to run in the background.
    command: String,
    /// Short human-readable description, surfaced with every event.
    description: String,
    /// Working directory for the command. Defaults to the turn cwd.
    #[serde(default)]
    workdir: Option<String>,
    /// Environment id from <environment_context>. Omit to use the primary environment.
    #[serde(default)]
    environment_id: Option<String>,
    /// Output token budget for the initial unified exec response.
    #[serde(default)]
    max_output_tokens: Option<usize>,
}

fn create_monitor_tool(include_environment_id: bool) -> ToolSpec {
    let mut properties = BTreeMap::from([
        (
            "command".to_string(),
            JsonSchema::string(Some(
                "Shell command to run through Codex sandboxing. Keep it selective, for example `tail -f app.log | grep --line-buffered ERROR`."
                    .to_string(),
            )),
        ),
        (
            "description".to_string(),
            JsonSchema::string(Some(
                "Short label shown with monitor output. Use 80 characters or fewer.".to_string(),
            )),
        ),
        (
            "workdir".to_string(),
            JsonSchema::string(Some(
                "Working directory for the command. Defaults to the turn cwd.".to_string(),
            )),
        ),
        (
            "max_output_tokens".to_string(),
            JsonSchema::number(Some(
                "Output token budget for the initial response. Monitor events are capped separately."
                    .to_string(),
            )),
        ),
    ]);

    if include_environment_id {
        properties.insert(
            "environment_id".to_string(),
            JsonSchema::string(Some(
                "Environment id from <environment_context>. Omit to use the primary environment."
                    .to_string(),
            )),
        );
    }

    ToolSpec::Function(ResponsesApiTool {
        name: MONITOR_TOOL_NAME.to_string(),
        description:
            "Start a sandboxed background command and deliver batched output as monitor notices. \
             Prefer exec_command for single-run commands and commands where completion is the only signal. \
             Use monitor only when intermediate output should wake the session. Do not acknowledge each notice; respond only when action is needed."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["command".to_string(), "description".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

fn validate_description(description: &str) -> Result<String, FunctionCallError> {
    let trimmed = description.trim();
    if trimmed.is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "monitor description must not be empty".to_string(),
        ));
    }
    if trimmed.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err(FunctionCallError::RespondToModel(format!(
            "monitor description must be {MAX_DESCRIPTION_CHARS} characters or fewer"
        )));
    }
    Ok(trimmed.to_string())
}

fn non_empty_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn exec_arguments_for_monitor(args: MonitorArgs) -> Result<String, FunctionCallError> {
    if args.command.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "monitor command must not be empty".to_string(),
        ));
    }

    let mut exec_args = Map::new();
    exec_args.insert("cmd".to_string(), Value::String(args.command));
    exec_args.insert(
        "yield_time_ms".to_string(),
        Value::Number(DEFAULT_MONITOR_YIELD_TIME_MS.into()),
    );
    exec_args.insert("tty".to_string(), Value::Bool(false));

    if let Some(workdir) = non_empty_optional(args.workdir) {
        exec_args.insert("workdir".to_string(), Value::String(workdir));
    }
    if let Some(environment_id) = non_empty_optional(args.environment_id) {
        exec_args.insert("environment_id".to_string(), Value::String(environment_id));
    }
    if let Some(max_output_tokens) = args.max_output_tokens {
        exec_args.insert(
            "max_output_tokens".to_string(),
            Value::Number(max_output_tokens.into()),
        );
    }

    Ok(Value::Object(exec_args).to_string())
}

impl ToolExecutor<ToolInvocation> for MonitorHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(MONITOR_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_monitor_tool(self.include_environment_id)
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            let ToolPayload::Function { arguments } = &invocation.payload else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "{MONITOR_TOOL_NAME} handler received unsupported payload"
                )));
            };
            let args: MonitorArgs = parse_arguments(arguments)?;
            let description = validate_description(&args.description)?;
            let exec_arguments = exec_arguments_for_monitor(args)?;
            let exec_invocation = ToolInvocation {
                payload: ToolPayload::Function {
                    arguments: exec_arguments,
                },
                ..invocation
            };

            self.exec_handler
                .handle_with_monitor(exec_invocation, description)
                .await
        })
    }
}

impl CoreToolRuntime for MonitorHandler {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_spec_is_function_with_required_args() {
        let ToolSpec::Function(tool) = MonitorHandler::default().spec() else {
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

    #[test]
    fn monitor_rejects_empty_description() {
        let err = validate_description("  ").expect_err("empty description rejected");
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn monitor_rejects_overlong_description() {
        let description = "x".repeat(MAX_DESCRIPTION_CHARS + 1);
        let err = validate_description(&description).expect_err("long description rejected");
        assert!(err.to_string().contains("characters or fewer"));
    }

    #[test]
    fn monitor_rejects_empty_command() {
        let args = MonitorArgs {
            command: " \t ".to_string(),
            description: "app errors".to_string(),
            workdir: None,
            environment_id: None,
            max_output_tokens: None,
        };

        let err = exec_arguments_for_monitor(args).expect_err("empty command rejected");
        assert!(err.to_string().contains("command must not be empty"));
    }

    #[test]
    fn monitor_exec_arguments_use_unified_exec_shape() {
        let args = MonitorArgs {
            command: "tail -f app.log".to_string(),
            description: "app errors".to_string(),
            workdir: Some("/tmp/project".to_string()),
            environment_id: Some("primary".to_string()),
            max_output_tokens: Some(123),
        };

        let value: Value =
            serde_json::from_str(&exec_arguments_for_monitor(args).expect("arguments"))
                .expect("json");

        assert_eq!(value["cmd"], "tail -f app.log");
        assert_eq!(value["yield_time_ms"], DEFAULT_MONITOR_YIELD_TIME_MS);
        assert_eq!(value["tty"], false);
        assert_eq!(value["workdir"], "/tmp/project");
        assert_eq!(value["environment_id"], "primary");
        assert_eq!(value["max_output_tokens"], 123);
    }

    #[test]
    fn monitor_exec_arguments_trim_optional_strings() {
        let args = MonitorArgs {
            command: "tail -f app.log".to_string(),
            description: "app errors".to_string(),
            workdir: Some("   ".to_string()),
            environment_id: Some(" primary ".to_string()),
            max_output_tokens: None,
        };

        let value: Value =
            serde_json::from_str(&exec_arguments_for_monitor(args).expect("arguments"))
                .expect("json");

        assert!(value.get("workdir").is_none());
        assert_eq!(value["environment_id"], "primary");
    }
}
