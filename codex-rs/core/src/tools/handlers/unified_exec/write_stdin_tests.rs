use pretty_assertions::assert_eq;

use super::*;

#[test]
fn empty_write_without_yield_uses_background_max_sentinel() {
    let args = WriteStdinArgs {
        session_id: 1,
        chars: String::new(),
        yield_time_ms: None,
        max_output_tokens: None,
    };

    assert_eq!(resolve_yield_time_ms(&args), u64::MAX);
}

#[test]
fn non_empty_write_without_yield_stays_responsive() {
    let args = WriteStdinArgs {
        session_id: 1,
        chars: "q".to_string(),
        yield_time_ms: None,
        max_output_tokens: None,
    };

    assert_eq!(
        resolve_yield_time_ms(&args),
        super::super::default_write_stdin_yield_time_ms()
    );
}

#[test]
fn explicit_yield_is_preserved() {
    let args = WriteStdinArgs {
        session_id: 1,
        chars: String::new(),
        yield_time_ms: Some(30_000),
        max_output_tokens: None,
    };

    assert_eq!(resolve_yield_time_ms(&args), 30_000);
}
