use pretty_assertions::assert_eq;

use super::MONITOR_MAX_LINE_BYTES;
use super::push_monitor_chunk;

#[test]
fn push_monitor_chunk_keeps_partial_tail_pending() {
    let mut pending = Vec::new();
    let mut lines = Vec::new();
    let mut omitted_bytes = 0;

    push_monitor_chunk(&mut pending, &mut lines, &mut omitted_bytes, b"one\ntwo");

    assert_eq!(lines, vec!["one"]);
    assert_eq!(pending, b"two");
    assert_eq!(omitted_bytes, 0);
}

#[test]
fn push_monitor_chunk_truncates_long_complete_line() {
    let mut pending = Vec::new();
    let mut lines = Vec::new();
    let mut omitted_bytes = 0;
    let line = "a".repeat(MONITOR_MAX_LINE_BYTES + 1);

    push_monitor_chunk(
        &mut pending,
        &mut lines,
        &mut omitted_bytes,
        format!("{line}\n").as_bytes(),
    );

    assert_eq!(pending, b"");
    assert_eq!(lines, vec!["a".repeat(MONITOR_MAX_LINE_BYTES)]);
    assert_eq!(omitted_bytes, 1);
}

#[test]
fn push_monitor_chunk_preserves_utf8_split_across_chunks() {
    let mut pending = Vec::new();
    let mut lines = Vec::new();
    let mut omitted_bytes = 0;
    let output = "ready: ✅\n".as_bytes();

    push_monitor_chunk(
        &mut pending,
        &mut lines,
        &mut omitted_bytes,
        &output[..output.len() - 2],
    );
    push_monitor_chunk(
        &mut pending,
        &mut lines,
        &mut omitted_bytes,
        &output[output.len() - 2..],
    );

    assert_eq!(lines, vec!["ready: ✅"]);
    assert_eq!(pending, b"");
    assert_eq!(omitted_bytes, 0);
}
