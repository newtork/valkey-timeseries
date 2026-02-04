use crate::aggregators::{AggregationHandler, Aggregator, calc_bucket_start};
use crate::common::logging::log_warning;
use crate::common::rdb::{RdbSerializable, rdb_load_timestamp, rdb_save_timestamp};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::index::{get_series_by_id, with_timeseries_postings};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesGuardMut, SeriesRef, TimeSeries};
use get_size2::GetSize;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use smallvec::SmallVec;
use std::cmp::Ordering;
use topo_sort::{SortResults, TopoSort};
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, raw};

const PARALLEL_THRESHOLD: usize = 2;
const TEMP_VEC_LEN: usize = 6;

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_start: Option<Timestamp>,
}

impl GetSize for CompactionRule {
    fn get_size(&self) -> usize {
        size_of::<SeriesRef>() // dest_id
            + self.aggregator.get_size()
            + size_of::<u64>() // bucket_duration
            + size_of::<Timestamp>() // align_timestamp
            + size_of::<Option<Timestamp>>() // bucket_start
    }
}

impl CompactionRule {
    pub(crate) fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        calc_bucket_start(ts, self.align_timestamp, self.bucket_duration)
    }

    pub(super) fn get_bucket_range(&self, ts: Timestamp) -> (Timestamp, Timestamp) {
        let start = self.calc_bucket_start(ts);
        let end = start.saturating_add_unsigned(self.bucket_duration);
        (start, end)
    }

    pub(super) fn reset(&mut self) {
        self.aggregator.reset();
        self.bucket_start = None;
    }
}

impl RdbSerializable for CompactionRule {
    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.rdb_save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.bucket_start.unwrap_or(-1));
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::rdb_load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let start_ts = rdb_load_timestamp(rdb)?;
        let bucket_start = if start_ts == -1 { None } else { Some(start_ts) };

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            bucket_start,
        })
    }
}

struct CompactionContext<'a> {
    parent: &'a TimeSeries,
    dest: &'a mut TimeSeries,
    rule: &'a mut CompactionRule,
    added: bool,
}

impl<'a> CompactionContext<'a> {
    fn new(parent: &'a TimeSeries, dest: &'a mut TimeSeries, rule: &'a mut CompactionRule) -> Self {
        Self {
            parent,
            dest,
            rule,
            added: false,
        }
    }
}

/// Single entry point for all compaction-related mutations.
#[derive(Debug, Clone, Copy)]
pub enum CompactionOp {
    /// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
    AddNew(Sample),
    /// Handle compaction for an upsert (timestamp <= last sample timestamp)
    Upsert(Sample),
    /// Remove a range from source and reflect it into destinations (and ongoing aggregation state)
    RemoveRange { start: Timestamp, end: Timestamp },
}

pub fn apply_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, op)
}

fn null_ts_filter(ts: Timestamp) -> bool {
    ts != 0
}

fn apply_op(ctx: &mut CompactionContext<'_>, op: CompactionOp) -> TsdbResult<()> {
    match op {
        CompactionOp::AddNew(sample) => handle_sample_compaction(ctx, sample),
        CompactionOp::Upsert(sample) => handle_compaction_upsert(ctx, sample),
        CompactionOp::RemoveRange { start, end } => {
            handle_compaction_range_removal(ctx, start, end)
        }
    }
}

/// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
fn handle_sample_compaction(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let sample_bucket_start = ctx.rule.calc_bucket_start(ts);

    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // First sample for this rule - initialize the aggregation
        ctx.rule.bucket_start = Some(sample_bucket_start);
        ctx.rule.aggregator.update(sample.timestamp, sample.value);
        return Ok(());
    };

    match sample_bucket_start.cmp(&current_bucket_start) {
        Ordering::Equal => {
            // Sample belongs to the current aggregation bucket
            ctx.rule.aggregator.update(sample.timestamp, sample.value);
        }
        Ordering::Greater => {
            // Sample starts a new bucket - finalize the current bucket first
            finalize_current_bucket(ctx, sample, sample_bucket_start)?;
        }
        Ordering::Less => {
            let bucket_end = sample_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
            // Sample is in an older bucket (shouldn't happen for new samples, but handle gracefully)
            recalculate_bucket(ctx, sample_bucket_start, bucket_end, null_ts_filter)?;
        }
    }

    Ok(())
}

/// Finalize the current aggregation bucket and start a new one
fn finalize_current_bucket(
    ctx: &mut CompactionContext<'_>,
    new_sample: Sample,
    new_bucket_start: Timestamp,
) -> TsdbResult<()> {
    // Finalize the current bucket
    let aggregated_value = ctx.rule.aggregator.finalize();
    let current_bucket_start = ctx.rule.bucket_start.expect(
        "finalize_current_bucket should be called when current bucket start is already set",
    );

    add_dest_bucket(ctx, current_bucket_start, aggregated_value)?;

    // Start a new bucket with the new sample
    ctx.rule
        .aggregator
        .update(new_sample.timestamp, new_sample.value);
    ctx.rule.bucket_start = Some(new_bucket_start);

    Ok(())
}

/// Handle upsert compaction for a destination series
/// This is called when a sample is being inserted/updated with a timestamp that's <= the last sample timestamp
fn handle_compaction_upsert(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let bucket_start = ctx.rule.calc_bucket_start(ts);

    // Check if this affects the current ongoing aggregation bucket
    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // No current bucket, this is the first sample for this rule
        ctx.rule.bucket_start = Some(bucket_start);
        ctx.rule.aggregator.update(sample.timestamp, sample.value);
        return Ok(());
    };

    let duration = ctx.rule.bucket_duration;
    let bucket_end = current_bucket_start.saturating_add_unsigned(duration);

    if bucket_start == current_bucket_start {
        // This sample belongs to the current aggregation bucket
        // We need to recalculate the entire bucket since we don't know what changed
        recalculate_current_bucket(ctx, current_bucket_start, bucket_end)?;
        return Ok(());
    }

    // This is a historical upsert - need to recalculate the affected bucket
    recalculate_bucket(ctx, bucket_start, bucket_end, null_ts_filter)
}

/// Recalculate the current ongoing aggregation bucket
fn recalculate_current_bucket(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
) -> TsdbResult<()> {
    // Reset the aggregator and recalculate from all samples in the bucket
    calculate_range(
        ctx.parent,
        &mut ctx.rule.aggregator,
        bucket_start,
        bucket_end - 1,
        null_ts_filter,
    );

    // reset would have cleared the bucket_start, so we need to set it again
    ctx.rule.bucket_start = Some(bucket_start);

    Ok(())
}

/// Recalculate a historical bucket and update the destination series
fn recalculate_bucket<F>(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    filter: F,
) -> TsdbResult<()>
where
    F: Fn(Timestamp) -> bool,
{
    // Create a new aggregator for this bucket
    let mut bucket_aggregator = ctx.rule.aggregator.clone();
    bucket_aggregator.reset();

    // Aggregate all samples in this bucket
    let has_samples = calculate_range(
        ctx.parent,
        &mut bucket_aggregator,
        bucket_start,
        bucket_end - 1,
        &filter,
    );

    if has_samples {
        let aggregated_value = bucket_aggregator.finalize();
        add_dest_bucket(ctx, bucket_start, aggregated_value)?;
    } else {
        // No samples in this bucket anymore, remove it from destination
        ctx.dest.remove_range(bucket_start, bucket_end - 1)?;
    }

    Ok(())
}

/// When a range of samples is removed, we need to remove samples in the corresponding
/// rule destination series that overlap with the range.
///
/// We need to handle the following scenarios:
/// - `Single Bucket Partial Removal`: When the removal range affects only part of a single aggregation bucket,
///   it recalculates the aggregation for the remaining samples.
/// - `Multiple Bucket Removal`: When the removal spans multiple buckets, we need to handle each bucket appropriately:
///   completely removing middle buckets and recalculating partial buckets at the boundaries.
/// - `Complete Bucket Removal`: When entire buckets are removed, remove the corresponding aggregated
///   samples from the destination series.
/// - `Current Aggregation State`: If there's an ongoing aggregation (indicated by `hi_ts`), adjust the
///   current aggregation state to account for the removed samples.
/// - `Error Handling`: Properly handle cases where destination series are missing or inaccessible.
///
fn handle_compaction_range_removal(
    ctx: &mut CompactionContext,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    // Update destination series buckets that overlap with [start, end]
    let first_bucket_start = ctx.rule.calc_bucket_start(start);
    let last_bucket_start = ctx.rule.calc_bucket_start(end);

    let mut current_bucket_start = first_bucket_start;
    while current_bucket_start <= last_bucket_start {
        let bucket_end = current_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

        let fully_covered = start <= current_bucket_start && end >= bucket_end;
        if fully_covered {
            if !ctx.dest.is_empty() {
                ctx.dest
                    .remove_range(current_bucket_start, bucket_end - 1)?;
            }
        } else {
            // Recalculate this bucket excluding removed timestamps.
            // If destination has no flushed buckets yet, this still correctly maintains the aggregator state.
            recalculate_bucket(ctx, current_bucket_start, bucket_end, |ts| {
                ts < start || ts > end
            })?;
        }

        current_bucket_start = bucket_end;
    }

    // Adjust current in-memory aggregation (ongoing bucket), if affected
    adjust_current_bucket_after_removal(ctx, start, end);

    Ok(())
}

fn adjust_current_bucket_after_removal(
    ctx: &mut CompactionContext<'_>,
    removal_start: Timestamp,
    removal_end: Timestamp,
) {
    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // No active aggregation, nothing to adjust
        return;
    };

    let current_bucket_end = current_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

    // Check if the removal affects the current aggregation bucket
    if removal_start < current_bucket_end && removal_end > current_bucket_start {
        let mut new_aggregator = ctx.rule.aggregator.clone();
        new_aggregator.reset();

        // Re-aggregate samples that are not in the removal range
        let bucket_samples = ctx
            .parent
            .range_iter(current_bucket_start, current_bucket_end)
            .filter(|sample| sample.timestamp < removal_start || sample.timestamp > removal_end);

        for sample in bucket_samples {
            new_aggregator.update(sample.timestamp, sample.value);
        }

        ctx.rule.aggregator = new_aggregator;
    }
}

/// Iterates through compaction rules (possibly in parallel) and applies the specified operation.
fn process_series_with_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    // Process current series compaction rules
    apply_rules_on_series(ctx, series, op)?;

    // Collect child series (one level deep)
    let child_series = series
        .rules
        .iter()
        .filter_map(|rule| get_destination_series(ctx, rule.dest_id));

    // Process child series
    for mut child in child_series {
        apply_rules_on_series(ctx, &mut child, op)?;
    }

    Ok(())
}

fn apply_rules_on_series(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    let destinations = get_compaction_series(ctx, series);
    if destinations.is_empty() || series.rules.is_empty() {
        return Ok(());
    }

    let mut rules = std::mem::take(&mut series.rules);
    let result = apply_rules_parallel_or_seq(ctx, series, &mut rules, destinations, op);
    series.rules = rules;
    result
}

/// Internal function that handles parallel/sequential execution of compaction rules.
fn apply_rules_parallel_or_seq(
    ctx: &Context,
    series: &TimeSeries,
    rules: &mut [CompactionRule],
    child_series: SmallVec<SeriesGuardMut, TEMP_VEC_LEN>,
    op: CompactionOp,
) -> TsdbResult<()> {
    if rules.is_empty() {
        return Ok(());
    }

    let mut destinations = rules.iter_mut().zip(child_series).collect::<Vec<_>>();

    let results: Vec<Result<Option<SeriesRef>, TsdbError>> =
        if destinations.len() >= PARALLEL_THRESHOLD {
            destinations
                .par_mut()
                .map(|(rule, dest_guard)| {
                    let mut cctx = CompactionContext::new(series, dest_guard, rule);
                    apply_op(&mut cctx, op).map(|_| {
                        if cctx.added {
                            Some(dest_guard.id)
                        } else {
                            None
                        }
                    })
                })
                .collect()
        } else {
            destinations
                .iter_mut()
                .map(|(rule, dest_guard)| {
                    let mut cctx = CompactionContext::new(series, dest_guard, rule);
                    apply_op(&mut cctx, op).map(|_| {
                        if cctx.added {
                            Some(dest_guard.id)
                        } else {
                            None
                        }
                    })
                })
                .collect()
        };

    let mut added: Vec<SeriesRef> = Vec::new();
    let mut first_error: Option<TsdbError> = None;

    for r in results {
        match r {
            Ok(Some(id)) => added.push(id),
            Ok(None) => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.clone());
                }
                let msg = format!("Failed to handle compaction rule for series: {error}");
                log_warning(msg);
            }
        }
    }

    if !added.is_empty() {
        notify_compaction(ctx, &added);
    }

    if let Some(err) = first_error {
        Err(err)
    } else {
        Ok(())
    }
}

pub(super) fn get_destination_series(
    ctx: &'_ Context,
    dest_id: SeriesRef,
) -> Option<SeriesGuardMut<'_>> {
    if let Ok(Some(res)) = get_series_by_id(ctx, dest_id, false, None)
        && res.is_compaction()
    {
        return Some(res);
    };
    ctx.log_verbose("Destination series for compaction not found or not a compaction series");
    None
}

fn get_compaction_series<'a>(
    ctx: &'a Context,
    series: &mut TimeSeries,
) -> SmallVec<SeriesGuardMut<'a>, TEMP_VEC_LEN> {
    if series.rules.is_empty() {
        return SmallVec::new();
    }

    let mut missing: SmallVec<_, TEMP_VEC_LEN> = SmallVec::new();
    let mut destinations: SmallVec<_, TEMP_VEC_LEN> = SmallVec::new();

    for rule in series.rules.iter() {
        if let Some(dest_series) = get_destination_series(ctx, rule.dest_id) {
            // Destination series exists, add it to the list
            destinations.push(dest_series);
        } else {
            // Destination series doesn't exist, mark rule for removal
            missing.push(rule.dest_id);
        }
    }

    if !missing.is_empty() {
        series.rules.retain(|r| !missing.contains(&r.dest_id));
    }
    destinations
}

fn notify_compaction(ctx: &Context, ids: &[SeriesRef]) {
    with_timeseries_postings(ctx, |postings| {
        for &id in ids {
            let Some(key) = postings.get_key_by_id(id) else {
                ctx.log_warning("Compaction notification failed: series key not found");
                continue;
            };
            let key = ctx.create_string(key.as_ref());
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add:dest", &key);
        }
    });
}

fn add_dest_bucket(ctx: &mut CompactionContext, ts: Timestamp, value: f64) -> TsdbResult<()> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    // Add the sample to the destination series
    // todo: specify to ignore whatever adjustments
    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(_) => {
            ctx.added = true;
            Ok(())
        }
        SampleAddResult::Ignored(_) => Ok(()), // duplicate sample, (ignored)
        SampleAddResult::TooOld => {
            // bucket start is too old, we cannot add it
            Ok(())
        }
        x => {
            let base_msg = format!("TSDB: failed to add sample @{ts} to destination bucket: {x}",);
            log_warning(base_msg.as_str());
            Err(TsdbError::General(base_msg))
        }
    }
}

fn calculate_range<F>(
    series: &TimeSeries,
    aggregator: &mut Aggregator,
    start: Timestamp,
    end: Timestamp,
    filter: F,
) -> bool
where
    F: Fn(Timestamp) -> bool,
{
    let mut has_samples = false;
    aggregator.reset();
    for sample in series
        .range_iter(start, end)
        .filter(|sample| filter(sample.timestamp))
    {
        aggregator.update(sample.timestamp, sample.value);
        has_samples = true;
    }
    has_samples
}

impl TimeSeries {
    pub fn add_compaction_rule(&mut self, rule: CompactionRule) {
        let mut rule = rule;
        if let Aggregator::Rate(r) = &mut rule.aggregator {
            r.set_window_ms(rule.bucket_duration);
        }
        self.rules.push(rule);
    }

    pub fn remove_compaction_rule(&mut self, dest_id: SeriesRef) -> Option<CompactionRule> {
        let Some(index) = self.rules.iter().position(|rule| rule.dest_id == dest_id) else {
            // No rule found for this destination ID
            return None;
        };
        Some(self.rules.remove(index))
    }

    pub fn get_rule_by_dest_id(&self, dest_id: SeriesRef) -> Option<&CompactionRule> {
        self.rules.iter().find(|rule| rule.dest_id == dest_id)
    }

    pub fn remove_range_with_compaction(
        &mut self,
        ctx: &Context,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        // Then remove the actual data from the source series
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        if deleted_count > 0 && !self.rules.is_empty() {
            apply_compaction(
                ctx,
                self,
                CompactionOp::RemoveRange {
                    start: start_ts,
                    end: end_ts,
                },
            )?;
        }

        Ok(deleted_count)
    }

    pub fn run_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::AddNew(value))
    }

    pub fn upsert_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::Upsert(value))
    }
}

pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let src_id = series.src_series?;
    let Ok(Some(parent)) = get_series_by_id(ctx, src_id, false, None) else {
        // No source series or it doesn't exist
        return None;
    };

    let rule = parent.get_rule_by_dest_id(series.id)?;
    let start = rule.bucket_start?;

    let mut agg = rule.aggregator.clone();
    let value = agg.finalize();

    let sample = Sample::new(start, value);
    Some(sample)
}

pub fn check_circular_dependencies(ctx: &Context, series: &mut TimeSeries) -> ValkeyResult<()> {
    let graph = build_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }
    Ok(())
}

/// Check if adding a new compaction rule would create a circular dependency
pub fn check_new_rule_circular_dependency(
    ctx: &Context,
    series: &mut TimeSeries,
    dest: &mut TimeSeries,
) -> ValkeyResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }

    let mut graph = build_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }

    // Check if the new rule would create a circular dependency
    // log_info(format!("candidate rule {} -> {}", series.id, dest.id));
    graph.insert(series.id, vec![dest.id]);
    build_dependency_graph_internal(ctx, dest, &mut graph)?;

    let SortResults::Full(_nodes) = graph.into_vec_nodes() else {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    };

    Ok(())
}

pub fn build_dependency_graph(
    ctx: &Context,
    series: &mut TimeSeries,
) -> ValkeyResult<TopoSort<SeriesRef>> {
    let mut graph = TopoSort::with_capacity(10);

    if !series.rules.is_empty() {
        build_dependency_graph_internal(ctx, series, &mut graph)?;
    }

    Ok(graph)
}

fn build_dependency_graph_internal(
    ctx: &Context,
    source_series: &mut TimeSeries,
    graph: &mut TopoSort<SeriesRef>,
) -> ValkeyResult<()> {
    let mut destinations = get_compaction_series(ctx, source_series);
    if destinations.is_empty() {
        return Ok(());
    }
    let dest_ids = destinations.iter().map(|x| x.id).collect::<Vec<_>>();
    graph.insert(source_series.id, dest_ids);
    if graph.cycle_detected() {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    }
    for dest in destinations.iter_mut() {
        build_dependency_graph_internal(ctx, dest, graph)?;
    }
    Ok(())
}
