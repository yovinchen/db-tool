use super::Context;
use dbtool_core::{port::CapabilityReport, Result};

pub async fn run(ctx: &Context) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let report = CapabilityReport::new(conn.capabilities(), conn.operations());
    Ok(ctx.render_success(conn.kind().0.as_str(), report, 0, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::port::{Capabilities, CapabilityOperation};

    #[test]
    fn operation_output_is_sorted_and_deduplicated_by_stable_name() {
        let operations = CapabilityReport::new(
            Capabilities::default(),
            vec![
                CapabilityOperation::SqlQuery,
                CapabilityOperation::KeyValueGet,
                CapabilityOperation::SqlQuery,
                CapabilityOperation::DocumentFind,
            ],
        )
        .operations;

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
