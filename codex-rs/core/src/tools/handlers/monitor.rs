use std::collections::BTreeMap;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::rewrite_function_string_argument;
use crate::tools::handlers::unified_exec::ExecCommandHandler;
use crate::tools::handlers::unified_exec::ExecCommandHandlerOptions;
use crate::tools::handlers::unified_exec::post_unified_exec_tool_use_payload;
use crate::tools::handlers::updated_hook_command;
use crate::tools::hook_names::HookToolName;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::PostToolUsePayload;
use crate::tools::registry::PreToolUsePayload;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::MonitorConfig;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;

const MONITOR_TOOL_NAME: &str = "monitor";
const DEFAULT_MONITOR_YIELD_TIME_MS: u64 = 250;
const DEFAULT_MONITOR_TIMEOUT_MS: u64 = 300_000;
const MAX_MONITOR_TIMEOUT_MS: u64 = 3_600_000;
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
    /// Stop the monitor after this many milliseconds. Defaults to five minutes.
    #[serde(default)]
    timeout_ms: Option<u64>,
    /// Keep the monitor alive for the session instead of applying a deadline.
    #[serde(default)]
    persistent: bool,
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
        (
            "timeout_ms".to_string(),
            JsonSchema::integer(Some(format!(
                "Monitor lifetime in milliseconds. Defaults to {DEFAULT_MONITOR_TIMEOUT_MS} and must not exceed {MAX_MONITOR_TIMEOUT_MS}."
            ))),
        ),
        (
            "persistent".to_string(),
            JsonSchema::boolean(Some(
                    "Keep the monitor alive for the session. Stop it with write_stdin using Ctrl-C. The output-notification safety limit still applies."
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
             Use monitor only when intermediate output should wake the session. Monitors default to a five-minute lifetime. \
             Do not acknowledge each notice; respond only when action is needed."
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

fn monitor_config(args: &MonitorArgs) -> Result<MonitorConfig, FunctionCallError> {
    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_MONITOR_TIMEOUT_MS);
    if timeout_ms == 0 || timeout_ms > MAX_MONITOR_TIMEOUT_MS {
        return Err(FunctionCallError::RespondToModel(format!(
            "monitor timeout_ms must be between 1 and {MAX_MONITOR_TIMEOUT_MS}"
        )));
    }
    Ok(MonitorConfig {
        description: validate_description(&args.description)?,
        timeout: (!args.persistent).then(|| std::time::Duration::from_millis(timeout_ms)),
    })
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
            let monitor_config = monitor_config(&args)?;
            let exec_arguments = exec_arguments_for_monitor(args)?;
            let exec_invocation = ToolInvocation {
                payload: ToolPayload::Function {
                    arguments: exec_arguments,
                },
                ..invocation
            };

            self.exec_handler
                .handle_with_monitor(exec_invocation, monitor_config)
                .await
        })
    }
}

impl CoreToolRuntime for MonitorHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    fn pre_tool_use_payload(&self, invocation: &ToolInvocation) -> Option<PreToolUsePayload> {
        let ToolPayload::Function { arguments } = &invocation.payload else {
            return None;
        };

        parse_arguments::<MonitorArgs>(arguments)
            .ok()
            .map(|args| PreToolUsePayload {
                tool_name: HookToolName::bash(),
                tool_input: serde_json::json!({ "command": args.command }),
            })
    }

    fn with_updated_hook_input(
        &self,
        mut invocation: ToolInvocation,
        updated_input: Value,
    ) -> Result<ToolInvocation, FunctionCallError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(FunctionCallError::RespondToModel(
                "hook input rewrite received unsupported monitor payload".to_string(),
            ));
        };
        invocation.payload = ToolPayload::Function {
            arguments: rewrite_function_string_argument(
                &arguments,
                MONITOR_TOOL_NAME,
                "command",
                updated_hook_command(&updated_input)?,
            )?,
        };
        Ok(invocation)
    }

    fn post_tool_use_payload(
        &self,
        invocation: &ToolInvocation,
        result: &dyn crate::tools::context::ToolOutput,
    ) -> Option<PostToolUsePayload> {
        post_unified_exec_tool_use_payload(invocation, result)
    }
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
