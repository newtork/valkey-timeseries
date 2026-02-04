mod add;
mod alter_series;
mod card;
mod card_fanout_operation;
pub mod command_args;
mod create_rule;
mod create_series;
mod del;
mod delete_rule;
mod fanout;
mod get;
mod incr_decr_by;
mod info;
mod ingest;
mod join;
mod label_names;
mod label_names_fanout_operation;
mod label_values;
mod label_values_fanout_operation;
mod madd;
mod mdel;
mod mdel_fanout_operation;
mod mget;
mod mget_fanout_operation;
mod mrange;
mod mrange_fanout_operation;
mod query_index;
mod query_index_fanout_operation;
mod range;
mod stats;
mod stats_fanout_operation;
mod utils;

pub use command_args::*;
pub use create_rule::*;
pub use delete_rule::*;

pub use add::*;
pub use alter_series::*;
pub use card::*;
pub use create_series::*;
pub use del::*;
pub use get::*;
pub use incr_decr_by::*;
pub use info::*;
pub use ingest::*;
pub use join::*;
pub use label_names::*;
pub use label_values::*;
pub use madd::*;
pub use mdel::*;
pub use mget::*;
pub use mrange::*;
pub use query_index::*;
pub use range::*;
pub use stats::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use card_fanout_operation::CardFanoutOperation;
use label_names_fanout_operation::LabelNamesFanoutOperation;
use label_values_fanout_operation::LabelValuesFanoutOperation;
use mdel_fanout_operation::MDelFanoutOperation;
use mget_fanout_operation::MGetFanoutOperation;
use mrange_fanout_operation::MRangeFanoutOperation;
use query_index_fanout_operation::QueryIndexFanoutOperation;
use stats_fanout_operation::StatsFanoutOperation;

pub(crate) fn register_fanout_operations() -> ValkeyResult<()> {
    register_fanout_operation::<StatsFanoutOperation>()?;
    register_fanout_operation::<CardFanoutOperation>()?;
    register_fanout_operation::<LabelNamesFanoutOperation>()?;
    register_fanout_operation::<LabelValuesFanoutOperation>()?;
    register_fanout_operation::<MDelFanoutOperation>()?;
    register_fanout_operation::<MGetFanoutOperation>()?;
    register_fanout_operation::<MRangeFanoutOperation>()?;
    register_fanout_operation::<QueryIndexFanoutOperation>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::commands::register_fanout_operations;

    #[test]
    fn test_register_fanout_operations() {
        let result = register_fanout_operations();
        assert!(result.is_ok());
    }
}
