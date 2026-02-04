#![allow(dead_code)]
extern crate enum_dispatch;
extern crate get_size2;
#[cfg(test)]
extern crate serial_test;
extern crate strum;
extern crate strum_macros;
extern crate valkey_module_macros;

use crate::commands::register_fanout_operations;
use crate::common::threads::init_thread_pool;
use crate::config::register_config;
use crate::fanout::{init_fanout, is_clustered};
use logger_rust::{LogLevel, set_log_level};
use std::sync::atomic::AtomicBool;
use valkey_module::{Context, Status, ValkeyString, Version, valkey_module};

pub mod aggregators;
pub(crate) mod commands;
pub mod common;
pub mod config;
mod error;
pub mod error_consts;
mod fanout;
pub mod iterators;
mod join;
mod labels;
mod parser;
mod series;
mod server_events;
mod tests;

use crate::series::background_tasks::init_background_tasks;
use crate::series::index::init_croaring_allocator;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::server_events::{generic_key_events_handler, register_server_events};

pub const VK_TIMESERIES_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "ts";

static IS_MODULE_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn is_module_initialized() -> bool {
    IS_MODULE_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn valid_server_version(version: Version) -> bool {
    let server_version = &[
        version.major.into(),
        version.minor.into(),
        version.patch.into(),
    ];
    server_version >= config::TIMESERIES_MIN_SUPPORTED_VERSION
}

fn preload(ctx: &Context, args: &[ValkeyString]) -> Status {
    // perform preload validations here, useful for MODULE LOAD
    // unlike init which is called at the end of the valkey_module! macro this is called at the beginning
    let version = ctx.get_server_version().unwrap();
    ctx.log_notice(&format!(
        "preload for server version {version:?} with args: {args:?}"
    ));

    let ver = ctx
        .get_server_version()
        .expect("Unable to get server version!");

    if !valid_server_version(ver) {
        ctx.log_warning(
            format!(
                "The minimum supported Valkey server version for the valkey-timeseries module is {:?}",
                config::TIMESERIES_MIN_SUPPORTED_VERSION
            )
                .as_str(),
        );
        return Status::Err;
    }

    // respond with either Status::Ok or Status::Err (if you want to prevent module loading)
    Status::Ok
}

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    init_croaring_allocator();

    set_log_level(LogLevel::Console);

    if let Err(e) = register_config(ctx, args) {
        let msg = format!("Failed to register config: {e}");
        ctx.log_warning(&msg);
        return Status::Err;
    }

    if is_clustered(ctx) {
        init_fanout(ctx);
        if let Err(e) = register_fanout_operations() {
            let msg = format!("Failed to register fanout operations: {e}");
            ctx.log_warning(&msg);
            return Status::Err;
        };
    }

    if let Err(e) = register_server_events(ctx) {
        let msg = format!("Failed to register server events: {e}");
        ctx.log_warning(&msg);
        return Status::Err;
    }

    init_thread_pool();
    init_background_tasks(ctx);

    ctx.log_notice("valkey-timeseries module initialized");
    IS_MODULE_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
    Status::Ok
}

fn deinitialize(ctx: &Context) -> Status {
    ctx.log_notice("deinitialize");
    IS_MODULE_INITIALIZED.store(false, std::sync::atomic::Ordering::Relaxed);
    Status::Ok
}

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        valkey_module::alloc::ValkeyAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

valkey_module! {
    name: MODULE_NAME,
    version: VK_TIMESERIES_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VK_TIME_SERIES_TYPE],
    preload: preload,
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "timeseries",
    ]
    commands: [
        ["TS.CREATE", commands::create, "write deny-oom", 1, 1, 1, "write fast timeseries"],
        ["TS.ALTER", commands::alter_series, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.ADD", commands::add, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.GET", commands::get, "readonly fast", 1, 1, 1, "fast read timeseries"],
        ["TS.MGET", commands::mget, "readonly fast", 0, 0, -1, "fast read timeseries"],
        ["TS.MADD", commands::madd, "write deny-oom", 1, -1, 3, "fast write timeseries"],
        ["TS.DEL", commands::del, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.DECRBY", commands::decrby, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.INCRBY", commands::incrby, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.INGEST", commands::ingest, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.JOIN", commands::join, "readonly", 1, 2, 1, "read timeseries"],
        ["TS.MDEL", commands::mdel, "write deny-oom", 0, 0, -1, "write timeseries"],
        ["TS.MRANGE", commands::mrange, "readonly", 0, 0, -1, "read timeseries"],
        ["TS.MREVRANGE", commands::mrevrange, "readonly", 0, 0, -1, "read timeseries"],
        ["TS.RANGE", commands::range, "readonly", 1, 1, 1, "read timeseries"],
        ["TS.REVRANGE", commands::rev_range, "readonly", 1, 1, 1, "read timeseries"],
        ["TS.INFO", commands::info, "readonly", 0, 0, 0, "read fast timeseries"],
        ["TS.QUERYINDEX", commands::query_index, "readonly", 0, 0, 0, "read timeseries"],
        ["TS.CARD", commands::cardinality, "readonly", 0, 0, 0, "read timeseries"],
        ["TS.LABELNAMES", commands::label_names, "readonly", 0, 0, 0, "read timeseries"],
        ["TS.LABELVALUES", commands::label_values, "readonly", 0, 0, 0, "read timeseries"],
        ["TS.STATS", commands::stats, "readonly", 0, 0, 0, "read timeseries"],
        ["TS.CREATERULE", commands::create_rule, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.DELETERULE", commands::delete_rule, "write deny-oom", 1, 1, 1, "write timeseries"],
    ]
    event_handlers: [
        [@GENERIC @LOADED @TRIMMED: generic_key_events_handler]
    ]
}
