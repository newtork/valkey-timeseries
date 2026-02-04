use crate::commands::parse_series_options;
use crate::series::{
    DuplicatePolicy, IngestedSamples, TimeSeries, bulk_insert_samples, create_and_store_series,
    get_timeseries_mut,
};
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.INGEST key data
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [ON_DUPLICATE policy_ovr]
///     [ENCODING <COMPRESSED|UNCOMPRESSED>]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///
pub fn ingest(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key = args[1].clone();
    // I don't like the copying here, but we need a mutable buffer
    let buf = {
        let data = args[2].as_slice();
        data.to_vec()
    };

    if let Some(mut guard) = get_timeseries_mut(ctx, &key, false, Some(AclPermissions::UPDATE))? {
        // args.done()?;
        return handle_ingest(ctx, &mut guard, buf);
    }

    let options = parse_series_options(args, 4, &[])?;

    let mut series = create_and_store_series(ctx, &key, options, true, true)?;

    let val = handle_ingest(ctx, &mut series, buf);
    ctx.replicate_verbatim();
    val
}

fn handle_ingest(ctx: &Context, series: &mut TimeSeries, buf: Vec<u8>) -> ValkeyResult {
    let mut buf = buf;

    let sample_data = IngestedSamples::from_json_lines(&mut buf)?;

    let samples = &sample_data.samples;
    let duplicate_policy = series
        .sample_duplicates
        .resolve_policy(Some(DuplicatePolicy::Block));

    let results = bulk_insert_samples(ctx, series, samples, Some(duplicate_policy));

    let success_count = results.iter().filter(|res| res.is_ok()).count();

    let result = vec![
        ValkeyValue::Integer(success_count as i64),
        ValkeyValue::Integer(sample_data.samples.len() as i64),
    ];

    Ok(ValkeyValue::Array(result))
}
