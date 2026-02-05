//! Ingest samples at ludicrous speed.
//!
//! This module provides bulk insertion of samples into a time series, with support for duplicate
//! policies and automatic compaction handling. It is optimized for high-throughput data ingestion
//! scenarios by leveraging parallel processing and efficient sample merging.
use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::error_consts;
use crate::series::chunks::{Chunk, TimeSeriesChunk};
use crate::series::compaction::get_destination_series;
use crate::series::index::with_timeseries_postings;
use crate::series::{CompactionRule, DuplicatePolicy, SampleAddResult, SeriesRef, TimeSeries};
use orx_parallel::{IterIntoParIter, ParIter, ParallelizableCollection};
use range_set_blaze::RangeSetBlaze;
use simd_json::base::{ValueAsArray, ValueAsScalar};
use simd_json::borrowed::Value;
use simd_json::prelude::ValueObjectAccess;
use std::ops::RangeInclusive;
use std::sync::Mutex;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult};

pub const MAX_SAMPLES_PER_INSERT: usize = 1_000;
const COMPRESSION_RATIO_CONSERVATIVE: f64 = 2.0;
const EARLY_CHUNK_CAPACITY_FACTOR: f64 = 0.7;
const EARLY_CHUNK_SAMPLE_THRESHOLD: usize = 10;

#[derive(Debug)]
pub struct IngestedSamples {
    pub metric_name: String,
    pub key: String,
    pub samples: Vec<Sample>,
}

impl IngestedSamples {
    pub fn from_json_lines(input: &mut [u8]) -> ValkeyResult<Self> {
        let v: Value = simd_json::to_borrowed_value(input)?;

        let values_arr = v
            .get("values")
            .and_then(|v| v.as_array())
            .ok_or(ValkeyError::Str("TSDB: missing values"))?;

        let timestamps_arr = v
            .get("timestamps")
            .and_then(|t| t.as_array())
            .ok_or(ValkeyError::Str("TSDB: missing timestamps"))?;

        if timestamps_arr.is_empty() || values_arr.is_empty() {
            return Err(ValkeyError::Str("TSDB: no timestamps or values"));
        }

        if timestamps_arr.len() != values_arr.len() {
            return Err(ValkeyError::Str(
                "TSDB: timestamps and values length mismatch",
            ));
        }

        if values_arr.len() > MAX_SAMPLES_PER_INSERT {
            return Err(ValkeyError::Str(error_consts::TOO_MANY_SAMPLES));
        }

        let mut samples = Vec::with_capacity(values_arr.len());
        for (val, ts) in values_arr.iter().zip(timestamps_arr.iter()) {
            let value = val
                .cast_f64()
                .ok_or(ValkeyError::Str("TSDB: invalid value (expected number)"))?;

            let timestamp_u64 = ts
                .as_u64()
                .ok_or(ValkeyError::Str("TSDB: invalid timestamp (expected u64)"))?;

            samples.push(Sample {
                timestamp: timestamp_u64 as i64,
                value,
            });
        }

        samples.sort_by_key(|s| s.timestamp);

        Ok(IngestedSamples {
            metric_name: String::new(),
            key: String::new(),
            samples,
        })
    }
}

/// A holder for either an existing chunk reference or new chunk info.
#[derive(Debug, Copy, Clone)]
enum ChunkHolder {
    ExistingIdx(usize),
    New,
}

/// A grouped view of a contiguous sub-slice of `samples` that all map to the same destination chunk.
#[derive(Debug)]
struct ChunkSampleGroup<'a> {
    /// The chunk to insert samples into. None indicates a new chunk needs to be created.
    pub chunk: ChunkHolder,
    pub samples: &'a [Sample],
}

#[inline]
fn exec_merge(
    chunk: &mut TimeSeriesChunk,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    chunk.merge_samples(samples, policy).unwrap_or_else(|_| {
        let err = SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE);
        std::iter::repeat_n(err, samples.len()).collect()
    })
}

#[inline]
fn calculate_capacity(chunk: &TimeSeriesChunk) -> usize {
    let mut capacity = chunk.estimate_remaining_sample_capacity();

    if capacity > 0 && chunk.len() < EARLY_CHUNK_SAMPLE_THRESHOLD && chunk.is_compressed() {
        capacity = (capacity as f64 * EARLY_CHUNK_CAPACITY_FACTOR).floor() as usize;
        capacity = capacity.max(1);
    }

    capacity
}

fn get_min_allowed_timestamp(series: &TimeSeries) -> Timestamp {
    if series.retention.is_zero() {
        0
    } else {
        series.get_min_timestamp()
    }
}

fn add_chunks_for_remaining_samples<'a>(
    dest: &mut Vec<ChunkSampleGroup<'a>>,
    series: &TimeSeries,
    samples: &'a [Sample],
) {
    if samples.is_empty() {
        return;
    }

    let is_compressed = series.is_compressed();
    // based on max chunk size and encoding, estimate capacity
    let chunk_size = series.chunk_size_bytes;
    // if we're compressed, estimate how many samples per chunk based on a conservative compression ratio of 2:1
    let sample_size = size_of::<Sample>();
    let sample_capacity = if is_compressed {
        chunk_size / (sample_size / 2)
    } else {
        // uncompressed, so each sample is 16 bytes (8 bytes timestamp + 8 bytes value)
        chunk_size / sample_size
    };

    // chunk the samples into new chunks
    for slice in samples.chunks(sample_capacity) {
        dest.push(ChunkSampleGroup {
            chunk: ChunkHolder::New,
            samples: slice,
        });
    }
}

/// Groups a sorted `samples` slice into the chunk they would belong to if inserted.
///
/// Rules:
/// - `samples` must be sorted by `timestamp` ascending.
/// - Samples older than the retention window are ignored.
/// - Samples newer than the series' current last timestamp are pinned to the series' last chunk.
/// - If a chunk is at capacity, splits the samples and uses pre-created chunks for overflow.
/// - Returns groups in ascending chunk order, each group borrowing from the input slice.
///
/// # Arguments
/// * `series` - The time series to group samples into
/// * `samples` - The samples to group
fn group_samples_by_chunk<'a>(
    series: &TimeSeries,
    samples: &'a [Sample],
) -> Vec<ChunkSampleGroup<'a>> {
    if samples.is_empty() {
        return Vec::new();
    }

    let min_allowed_ts = get_min_allowed_timestamp(series);

    // Skip samples older than retention (drop them).
    let mut start_idx = 0usize;
    while start_idx < samples.len() && samples[start_idx].timestamp < min_allowed_ts {
        start_idx += 1;
    }
    if start_idx >= samples.len() {
        return Vec::new();
    }
    let samples = &samples[start_idx..];

    // Pre-allocate based on chunk count + potential new chunks
    let estimated_groups = series.chunks.len().saturating_add(2);
    let mut out: Vec<ChunkSampleGroup<'a>> = Vec::with_capacity(estimated_groups);

    // handle samples older than the first existing chunk by creating new chunk(s).
    let first_chunk_start = series
        .chunks
        .first()
        .map(|c| c.first_timestamp())
        .unwrap_or(min_allowed_ts);

    let mut i = 0usize;

    if i < samples.len() && samples[i].timestamp < first_chunk_start {
        let start = i;
        while i < samples.len() && samples[i].timestamp < first_chunk_start {
            i += 1;
        }
        add_chunks_for_remaining_samples(&mut out, series, &samples[start..i]);

        if i >= samples.len() {
            return out;
        }
    }

    // Empty series: create new chunks from all remaining samples.
    if series.is_empty() {
        add_chunks_for_remaining_samples(&mut out, series, &samples[i..]);
        return out;
    }

    let chunk_count = series.chunks.len();
    let last_index = chunk_count.saturating_sub(1);

    for (chunk_idx, chunk) in series.chunks.iter().enumerate() {
        let start = i;

        while i < samples.len() && chunk.is_timestamp_in_range(samples[i].timestamp) {
            i += 1;
        }

        if start < i {
            out.push(ChunkSampleGroup {
                chunk: ChunkHolder::ExistingIdx(chunk_idx),
                samples: &samples[start..i],
            });
        }

        if i < samples.len() && chunk_idx == last_index {
            add_chunks_for_remaining_samples(&mut out, series, &samples[i..]);
            return out;
        }
    }

    out
}

/// Produces disjoint `&mut` references for unique indices.
///
/// # Safety Requirements
/// Callers MUST ensure that `indices`:
/// - Are all within bounds of `slice`
/// - Are strictly unique (no duplicates)
/// - Are sorted in non-decreasing order
///
/// Violating these invariants leads to undefined behavior.
///
/// # Panics
/// Panics in debug builds if indices are not non-decreasing.
fn disjoint_get_many_mut_with_pos<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut out: Vec<&'a mut T> = Vec::with_capacity(indices.len());
    let mut base = 0usize;
    let mut tail: &'a mut [T] = slice;

    for &idx in indices {
        debug_assert!(idx >= base, "indices must be non-decreasing after sort");
        let rel = idx - base;
        let (_left, rest) = tail.split_at_mut(rel);
        let (item, rest) = rest.split_at_mut(1);
        out.push(&mut item[0]);
        tail = rest;
        base = idx + 1;
    }

    out
}

/// Parallel merge implementation:
/// - existing-chunk groups: merge in parallel by taking disjoint `&mut` borrows
/// - new-chunk groups: create and merge in parallel, then append sequentially
pub fn bulk_insert_samples(
    ctx: &Context,
    series: &mut TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    let _saved_sample_count = series.total_samples;
    let groups = group_samples_by_chunk(series, samples);

    if groups.is_empty() {
        return Vec::new();
    }

    // separate groups into existing-chunk and new-chunk categories.
    let (new_groups, existing_groups): (Vec<_>, Vec<_>) = groups
        .iter()
        .enumerate()
        .partition(|(_, g)| matches!(g.chunk, ChunkHolder::New));

    let existing_groups: Vec<_> = existing_groups
        .into_iter()
        .filter_map(|(pos, g)| {
            if let ChunkHolder::ExistingIdx(idx) = g.chunk {
                Some((pos, idx, g.samples))
            } else {
                None
            }
        })
        .collect();

    let new_groups: Vec<_> = new_groups
        .into_iter()
        .map(|(pos, g)| (pos, g.samples))
        .collect();

    // Prepare result slots.
    let mut group_results: Vec<Option<Vec<SampleAddResult>>> = vec![None; groups.len()];

    // Merge into existing chunks in parallel.
    if !existing_groups.is_empty() {
        let chunk_indices: Vec<usize> = existing_groups.iter().map(|(_, idx, _)| *idx).collect();
        let chunk_refs =
            disjoint_get_many_mut_with_pos(series.chunks.as_mut_slice(), &chunk_indices);

        let existing_results: Vec<(usize, Vec<SampleAddResult>)> = chunk_refs
            .into_iter()
            .zip(existing_groups.iter())
            .iter_into_par()
            .map(|(chunk, &(group_pos, _, samples))| {
                let res = exec_merge(chunk, samples, policy);
                (group_pos, res)
            })
            .collect();

        for (group_pos, res) in existing_results {
            group_results[group_pos] = Some(res);
        }
    }

    // Process new-chunk groups in parallel (each creates its own chunk).
    if !new_groups.is_empty() {
        let encoding = series.chunk_compression;
        let chunk_size = series.chunk_size_bytes;
        let new_results: Vec<(usize, TimeSeriesChunk, Vec<SampleAddResult>)> = new_groups
            .par()
            .map(|&(group_pos, samples)| {
                let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
                let res = exec_merge(&mut chunk, samples, policy);
                (group_pos, chunk, res)
            })
            .collect::<Vec<_>>();

        let chunk_count_before = series.chunks.len();

        for (group_pos, chunk, res) in new_results {
            group_results[group_pos] = Some(res);
            series.chunks.push(chunk);
        }

        // append new chunks to the series
        if series.chunks.len() != chunk_count_before {
            // Sort chunks by the first timestamp to maintain order.
            series.chunks.sort_by_key(|c| c.first_timestamp());
            series.update_first_last_timestamps();
        }
    }

    series.split_chunks_if_needed().unwrap_or_else(|e| {
        ctx.log_warning(&format!(
            "Failed to split chunks after bulk insert samples: {}",
            e
        ))
    });

    // make sure total_samples is accurate
    series.recalculate_total_samples();

    // flatten results in group order.
    let mut results: Vec<SampleAddResult> = Vec::with_capacity(samples.len());
    for opt in group_results.into_iter().flatten() {
        results.extend(opt);
    }

    #[cfg(not(test))]
    if series.total_samples > _saved_sample_count {
        let event = if series.is_compaction() {
            "ts.add:dest"
        } else {
            "ts.add"
        };
        notify_added(ctx, event, &[series.id]);
    }

    // compactions remain sequential because they mutate `series`.
    if !series.rules.is_empty() {
        let added: Vec<Sample> = results
            .iter()
            .filter_map(|res| {
                if let SampleAddResult::Ok(s) = res {
                    Some(*s)
                } else {
                    None
                }
            })
            .collect();

        if let Err(e) = run_compactions(ctx, series, &added) {
            ctx.log_warning(&format!(
                "Failed to run compactions after bulk insert samples: {e:?}"
            ))
        }
    }

    results
}

/// Run compactions for `series` from a pre-sorted `samples` vec.
///
/// This rebuilds each rule's destination series affected by `samples` by computing per-bucket aggregations.
///
/// Assumptions:
/// - `samples` is sorted by `timestamp` ascending.
/// - Destination series already exist and are compaction series (rules with missing dest series are dropped elsewhere).
pub fn run_compactions(
    ctx: &Context,
    series: &mut TimeSeries,
    samples: &[Sample],
) -> TsdbResult<()> {
    if series.rules.is_empty() || samples.is_empty() {
        return Ok(());
    }

    // borrow rules to avoid issues with mutable borrow later
    let mut rules = std::mem::take(&mut series.rules);

    // get all compactions per rule in parallel
    let mut rule_compactions = Vec::new();
    for rule in rules.iter_mut() {
        let compacted = get_compacted_samples(series, rule, samples);
        rule_compactions.push(compacted);
    }

    // restore rules
    series.rules = rules;

    for (compacted_samples, rule) in rule_compactions.into_iter().zip(series.rules.iter()) {
        let Some(mut dest_series) = get_destination_series(ctx, rule.dest_id) else {
            continue;
        };

        bulk_insert_samples(
            ctx,
            &mut dest_series,
            &compacted_samples,
            Some(DuplicatePolicy::KeepLast),
        );
    }

    Ok(())
}

fn get_compacted_samples(
    source: &TimeSeries,
    rule: &mut CompactionRule,
    src_samples: &[Sample],
) -> Vec<Sample> {
    let ranges = collect_input_ranges(rule, src_samples);
    let updated_aggr: Mutex<Option<Aggregator>> = Mutex::new(None);

    let added = ranges
        .ranges()
        .iter_into_par()
        .map(|range| aggregate_compaction_range(source, rule, range))
        .map(|(samples, aggr_opt)| {
            if let Some(aggr_state) = aggr_opt {
                let mut aggr = updated_aggr.lock().unwrap();
                *aggr = Some(aggr_state);
            }
            samples
        })
        .flatten()
        .collect::<Vec<_>>();

    // Update the rule's open bucket state if needed
    let mut guard = updated_aggr.lock().unwrap();
    if let Some(updated) = guard.take() {
        // Update bucket_start to reflect the current open bucket
        rule.aggregator = updated;
        // note ! this means we should not emit for this rule !!!!
    }

    added
}

fn collect_input_ranges(rule: &CompactionRule, samples: &[Sample]) -> RangeSetBlaze<Timestamp> {
    let mut ranges = RangeSetBlaze::new();
    if samples.is_empty() {
        return ranges;
    }

    let gap_threshold = rule.bucket_duration as i64;
    let mut cur = rule.get_bucket_range(samples[0].timestamp);

    let push_cur = |ranges: &mut RangeSetBlaze<Timestamp>, cur: (Timestamp, Timestamp)| {
        ranges.ranges_insert(cur.0..=cur.1);
    };

    for s in &samples[1..] {
        let next = rule.get_bucket_range(s.timestamp);
        if next.0.saturating_sub(cur.1) <= gap_threshold {
            cur.1 = next.1;
        } else {
            push_cur(&mut ranges, cur);
            cur = next;
        }
    }

    push_cur(&mut ranges, cur);
    ranges
}

fn range_affects_open_bucket(rule: &CompactionRule, range: RangeInclusive<Timestamp>) -> bool {
    let start = *range.start();
    let _end = *range.end();

    // Check if the range overlaps with the rule's current open bucket
    let open_bucket_start = rule.bucket_start;
    let open_bucket_end =
        open_bucket_start.map(|bs| bs.saturating_add_unsigned(rule.bucket_duration));

    // Determine if we need to preserve the open bucket's state
    open_bucket_start
        .and_then(|obs| open_bucket_end.map(|obe| start >= obs && start < obe))
        .unwrap_or(false)
}

/// Aggregate samples in `source` over the given `range` according to `rule`.
fn aggregate_compaction_range(
    source: &TimeSeries,
    rule: &CompactionRule,
    range: RangeInclusive<Timestamp>,
) -> (Vec<Sample>, Option<Aggregator>) {
    let (start, end) = (*range.start(), *range.end());
    let range_starts_in_open_bucket = range_affects_open_bucket(rule, range.clone());

    let mut aggr = rule.aggregator.clone();
    if !range_starts_in_open_bucket {
        aggr.reset();
    }

    let mut samples_out = Vec::new();
    let mut current_bucket_start: Option<Timestamp> = None;

    for sample in source.range_iter(start, end) {
        let bucket_start = rule.calc_bucket_start(sample.timestamp);

        // Finalize the previous bucket if we've moved to a new one
        if let Some(prev_bucket) = current_bucket_start
            && bucket_start != prev_bucket
        {
            let is_open = rule.bucket_start == Some(prev_bucket);
            if !is_open {
                samples_out.push(Sample::new(prev_bucket, aggr.finalize()));
                aggr.reset();
            }
        }

        current_bucket_start = Some(bucket_start);
        aggr.update(sample.timestamp, sample.value);
    }

    // Handle final bucket
    if let Some(bucket) = current_bucket_start {
        let is_still_open = rule
            .bucket_start
            .map(|bs| bucket == bs && end < bs.saturating_add_unsigned(rule.bucket_duration))
            .unwrap_or(false);

        if is_still_open {
            return (samples_out, Some(aggr));
        }
        samples_out.push(Sample::new(bucket, aggr.finalize()));
    }

    (samples_out, None)
}

fn notify_added(ctx: &Context, event: &str, ids: &[SeriesRef]) {
    with_timeseries_postings(ctx, |postings| {
        for &id in ids {
            let Some(key) = postings.get_key_by_id(id) else {
                ctx.log_warning("Compaction notification failed: series key not found");
                continue;
            };
            let key = ctx.create_string(key.as_ref());
            ctx.notify_keyspace_event(NotifyEvent::MODULE, event, &key);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::generators::DataGenerator;
    use std::time::Duration;

    fn generate_random_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .interval(Duration::from_millis(1000))
            .decimal_digits(4)
            .build()
            .generate()
    }

    #[test]
    fn test_parse_json_line() {
        let mut data = br#"{
            "values": [1, 1, 1],
            "timestamps": [1549891472010, 1549891487724, 1549891503438]
        }"#
        .to_vec();

        let parsed = IngestedSamples::from_json_lines(&mut data).unwrap();
        assert_eq!(parsed.samples.len(), 3);
        assert_eq!(parsed.samples[0].value, 1.0);
        assert_eq!(parsed.samples[0].timestamp, 1549891472010);
        assert_eq!(parsed.samples[2].timestamp, 1549891503438);
    }

    #[test]
    fn test_parse_mismatched_lengths() {
        let mut data = br#"{
            "metric": {"__name__": "test"},
            "values": [1, 2],
            "timestamps": [1000]
        }"#
        .to_vec();

        assert!(IngestedSamples::from_json_lines(&mut data).is_err());
    }

    fn s(ts: i64, v: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: v,
        }
    }

    #[test]
    fn group_empty_samples_returns_empty() {
        let series = TimeSeries::default();
        let groups = group_samples_by_chunk(&series, &[]);
        assert!(groups.is_empty());
    }

    #[test]
    fn group_skips_samples_older_than_retention_min_timestamp() {
        let mut series = TimeSeries::default();
        // Create initial data so the series isn't empty and has retention-derived min timestamp.
        // Insert a sample to establish a baseline.
        bulk_insert_samples(&Context::dummy(), &mut series, &[s(1_000, 1.0)], None);

        let min_allowed = get_min_allowed_timestamp(&series);

        // One sample below a retention window, one inside.
        let samples = vec![s(min_allowed - 1, 1.0), s(min_allowed, 2.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        // All grouped samples must be >= min_allowed.
        let grouped: Vec<i64> = groups
            .iter()
            .flat_map(|g| g.samples.iter().map(|x| x.timestamp))
            .collect();
        assert!(grouped.iter().all(|&ts| ts >= min_allowed));
        assert!(grouped.contains(&min_allowed));
        assert!(!grouped.contains(&(min_allowed - 1)));
    }

    #[test]
    fn group_splits_samples_across_chunks_when_ranges_differ() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed series with enough samples to create multiple chunks.
        let seed: Vec<Sample> = (0..10_000).map(|i| s(i, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        assert!(series.chunks.len() >= 2);

        // Disable retention to ensure samples are not filtered.
        series.retention = Duration::ZERO;

        // Find two adjacent chunks where we can pick timestamps that are exclusive to each chunk.
        // This avoids relying on boundary timestamps and avoids assumptions about overlapping ranges.
        let mut chosen: Option<(i64, i64)> = None;

        for pair_start in (0..series.chunks.len() - 1).rev() {
            let c0 = &series.chunks[pair_start];
            let c1 = &series.chunks[pair_start + 1];

            let c0_first = c0.first_timestamp();
            let c0_last = c0.last_timestamp();
            let c1_first = c1.first_timestamp();
            let c1_last = c1.last_timestamp();

            let c0_mid = c0_first.saturating_add((c0_last.saturating_sub(c0_first)) / 2);
            let c1_mid = c1_first.saturating_add((c1_last.saturating_sub(c1_first)) / 2);

            let c0_candidates = [c0_first, c0_mid, c0_last];
            let c1_candidates = [c1_first, c1_mid, c1_last];

            let ts0_opt = c0_candidates
                .into_iter()
                .find(|&ts| c0.is_timestamp_in_range(ts) && !c1.is_timestamp_in_range(ts));

            let ts1_opt = c1_candidates
                .into_iter()
                .find(|&ts| c1.is_timestamp_in_range(ts) && !c0.is_timestamp_in_range(ts));

            if let (Some(ts0), Some(ts1)) = (ts0_opt, ts1_opt) {
                // Ensure sorted input for the grouping function contract.
                let (a, b) = if ts0 <= ts1 { (ts0, ts1) } else { (ts1, ts0) };
                chosen = Some((a, b));
                break;
            }
        }

        let (ts0, ts1) = chosen.expect(
            "Could not find adjacent chunks with non-overlapping (exclusive) timestamp membership; \
                 chunk range semantics may overlap. Adjust the test to assert weaker properties.",
        );

        let samples = vec![s(ts0, 123.0), s(ts1, 456.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        // Now it's valid to expect two groups: we've ensured the timestamps are exclusive.
        assert_eq!(groups.len(), 2);

        // Verify each group borrows a contiguous sub-slice of the input.
        let total_grouped: usize = groups.iter().map(|g| g.samples.len()).sum();
        assert_eq!(total_grouped, samples.len());
        assert_eq!(groups[0].samples, &samples[0..1]);
        assert_eq!(groups[1].samples, &samples[1..2]);
    }

    #[test]
    fn group_pins_samples_newer_than_last_chunk_to_last_chunk() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed series so it has at least one chunk.
        let seed: Vec<Sample> = (0..1_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Pick timestamps that are beyond the last chunk's max timestamp.
        let last_max = series.chunks.last().unwrap().last_timestamp();
        let samples = vec![s(last_max + 1, 1.0), s(last_max + 2, 2.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        assert_eq!(groups.len(), 1);

        assert_eq!(groups[0].samples.len(), 2);
        assert_eq!(groups[0].samples[0].timestamp, last_max + 1);
        assert_eq!(groups[0].samples[1].timestamp, last_max + 2);
    }

    fn collect_grouped_timestamps(groups: &[ChunkSampleGroup<'_>]) -> Vec<i64> {
        groups
            .iter()
            .flat_map(|g| g.samples.iter().map(|x| x.timestamp))
            .collect()
    }

    #[test]
    fn group_requires_sorted_input_preserves_contiguity_and_order() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed to avoid an empty-series special case.
        let seed: Vec<Sample> = (0..2_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Carefully sorted input.
        let samples = vec![s(5, 1.0), s(15, 2.0), s(25, 3.0), s(35, 4.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        let grouped = collect_grouped_timestamps(&groups);
        assert_eq!(grouped, vec![5, 15, 25, 35]);

        // Each group must borrow a contiguous sub-slice from `samples` (no reordering).
        let mut cursor = 0usize;
        for g in groups.iter() {
            assert!(!g.samples.is_empty());
            let len = g.samples.len();
            assert_eq!(&samples[cursor..cursor + len], g.samples);
            cursor += len;
        }
        assert_eq!(cursor, samples.len());
    }

    #[test]
    fn group_filters_all_samples_when_all_are_older_than_retention() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Create baseline data to establish a retention window.
        bulk_insert_samples(&ctx, &mut series, &[s(1_000, 1.0)], None);
        let min_allowed = get_min_allowed_timestamp(&series);

        let samples = vec![s(min_allowed - 100, 1.0), s(min_allowed - 1, 2.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        assert!(groups.is_empty());
    }

    #[test]
    fn bulk_insert_stores_samples_correctly_in_series() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples = generate_random_samples(120);

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // All samples should have been accepted.
        assert_eq!(results.len(), samples.len());
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // Series metadata should reflect stored samples.
        assert_eq!(series.total_samples, samples.len());
        assert_eq!(series.first_timestamp, samples[0].timestamp);
        assert_eq!(
            series.last_timestamp(),
            samples[samples.len() - 1].timestamp
        );

        // Stored samples should match exactly (timestamps and values) in order.
        let stored: Vec<Sample> = series.iter().collect();
        assert_eq!(stored.len(), samples.len());
        assert_eq!(stored, samples);
    }

    #[test]
    fn bulk_insert_large_batch_creates_multiple_chunks() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples: Vec<_> = generate_random_samples(10_000);
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert_eq!(results.len(), 10_000);
        let errors: Vec<&SampleAddResult> = results.iter().filter(|r| !r.is_ok()).collect();

        assert!(
            errors.is_empty(),
            "Some samples failed to insert: {:?}",
            errors
        );
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // make sure the series contains all samples
        assert_eq!(series.total_samples, samples.len());
        assert!(series.chunks.len() > 1);

        results.iter().zip(samples.iter()).for_each(|(r, s)| {
            if let SampleAddResult::Ok(sample) = r {
                assert_eq!(sample.timestamp, s.timestamp);
                assert_eq!(sample.value, s.value);
            } else {
                panic!("Expected all samples to be inserted successfully");
            }
        });

        for (sample, sample2) in series.iter().zip(samples.iter()) {
            assert_eq!(sample.timestamp, sample2.timestamp);
            assert_eq!(sample.value, sample2.value);
        }
    }

    #[test]
    fn bulk_insert_inserts_into_existing_chunks_and_creates_new_chunks_in_one_call() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed enough data to ensure we have at least one existing chunk with a well-defined range.
        let seed: Vec<Sample> = (0..5_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Make sure retention doesn't filter anything in this test.
        series.retention = Duration::ZERO;

        let chunks_before = series.chunks.len();
        assert!(chunks_before >= 1);

        // Pick a timestamp that we know lies inside an *existing* chunk.
        let c0 = &series.chunks[0];
        let existing_ts = c0
            .first_timestamp()
            .saturating_add((c0.last_timestamp().saturating_sub(c0.first_timestamp())) / 2);
        assert!(c0.is_timestamp_in_range(existing_ts));

        // Pick timestamps that are strictly newer than the last existing timestamp to force creation of new chunk(s).
        let last_max = series.chunks.last().unwrap().last_timestamp();
        let new_ts_1 = last_max + 10;
        let new_ts_2 = last_max + 20;

        let samples = vec![
            s(existing_ts, 123.0),
            s(new_ts_1, 456.0),
            s(new_ts_2, 789.0),
        ];

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // All inserts should succeed.
        assert_eq!(results.len(), samples.len());
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // We should have inserted into an existing chunk AND created at least one new chunk.
        assert!(series.chunks.len() > chunks_before);

        // Validate the timestamps are present in the series after insertion.
        let stored_ts: Vec<i64> = series.iter().map(|smpl| smpl.timestamp).collect();
        assert!(stored_ts.contains(&existing_ts));
        assert!(stored_ts.contains(&new_ts_1));
        assert!(stored_ts.contains(&new_ts_2));

        // Metadata sanity: last timestamp must advance to the newest inserted sample.
        assert_eq!(series.last_timestamp(), new_ts_2);
    }

    #[test]
    fn bulk_insert_updates_first_last_timestamps() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples = vec![s(1000, 1.0), s(2000, 2.0), s(3000, 3.0)];
        bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert_eq!(series.first_timestamp, 1000);
        assert_eq!(series.last_timestamp(), 3000);
    }
}
