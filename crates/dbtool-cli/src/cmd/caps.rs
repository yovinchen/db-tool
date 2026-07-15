use super::Context;
use dbtool_core::{
    port::{Capabilities, CapabilityOperation},
    Result,
};
use serde::Serialize;

#[derive(Serialize)]
struct CapabilityReport {
    #[serde(flatten)]
    legacy: Capabilities,
    operations: Vec<CapabilityOperation>,
}

fn sorted_operations(mut operations: Vec<CapabilityOperation>) -> Vec<CapabilityOperation> {
    operations.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    operations.dedup();
    operations
}

pub async fn run(ctx: &Context) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let report = CapabilityReport {
        legacy: conn.capabilities(),
        operations: sorted_operations(conn.operations()),
    };
    Ok(ctx.render_success(conn.kind().0.as_str(), report, 0, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_output_is_sorted_and_deduplicated_by_stable_name() {
        let operations = sorted_operations(vec![
            CapabilityOperation::SqlQuery,
            CapabilityOperation::KeyValueGet,
            CapabilityOperation::SqlQuery,
            CapabilityOperation::DocumentFind,
        ]);

        assert_eq!(
            operations,
            vec![
                CapabilityOperation::DocumentFind,
                CapabilityOperation::KeyValueGet,
                CapabilityOperation::SqlQuery,
            ]
        );
    }
}
