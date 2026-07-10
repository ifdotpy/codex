use pretty_assertions::assert_eq;

use super::MONITOR_MAX_BATCH_LINES;
use super::MONITOR_MAX_LINE_BYTES;
use super::MONITOR_MAX_PENDING_BYTES;
use super::push_monitor_chunk;
use super::take_ready_monitor_batch;

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

#[test]
fn ready_monitor_batch_contains_exactly_the_line_limit() {
    let mut lines = (0..=MONITOR_MAX_BATCH_LINES)
        .map(|index| index.to_string())
        .collect::<Vec<_>>();

    let batch = take_ready_monitor_batch(&mut lines).expect("ready batch");

    assert_eq!(batch.len(), MONITOR_MAX_BATCH_LINES);
    assert_eq!(lines, vec![MONITOR_MAX_BATCH_LINES.to_string()]);
    assert_eq!(take_ready_monitor_batch(&mut lines), None);
}

#[test]
fn push_monitor_chunk_bounds_unterminated_lines() {
    let mut pending = Vec::new();
    let mut lines = Vec::new();
    let mut omitted_bytes = 0;

    push_monitor_chunk(
        &mut pending,
        &mut lines,
        &mut omitted_bytes,
        &vec![b'a'; MONITOR_MAX_PENDING_BYTES + 1],
    );

    assert_eq!(pending, b"");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].ends_with(" [unterminated line truncated]"));
    assert_eq!(
        omitted_bytes,
        MONITOR_MAX_PENDING_BYTES + 1 - MONITOR_MAX_LINE_BYTES
    );
}
