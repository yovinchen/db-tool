use dbtool_core::registry::Registry;

/// Composition root: register adapters according to Cargo features (§6.3).
pub fn build_registry() -> Registry {
    // A deliberately empty registry is the contract for --no-default-features.
    #[allow(unused_mut)]
    let mut r = Registry::new();

    #[cfg(feature = "sql")]
    {
        use adapter_sql::{mysql_factory, postgres_factory, sqlite_factory};
        r.register_family("mysql", mysql_factory);
        r.register_family("postgres", postgres_factory);
        r.register("sqlite", sqlite_factory);
    }

    #[cfg(feature = "sqlserver")]
    {
        r.register_family("sqlserver", adapter_sqlserver::factory);
    }

    #[cfg(feature = "cassandra")]
    {
        r.register_family("cassandra", adapter_cassandra::factory);
    }

    #[cfg(feature = "db2")]
    {
        r.register_family("db2", adapter_db2::factory);
    }

    #[cfg(feature = "redis")]
    {
        r.register_family("redis", adapter_redis::factory);
    }

    #[cfg(feature = "mongodb")]
    {
        r.register("mongodb", adapter_mongo::factory);
    }

    #[cfg(feature = "search")]
    {
        r.register_family("opensearch", adapter_search::factory);
    }

    #[cfg(feature = "timeseries")]
    {
        r.register_family("prometheus", adapter_timeseries::factory);
    }

    #[cfg(any(feature = "kafka", feature = "kafka-native"))]
    {
        r.register_family("kafka", adapter_kafka::factory);
    }

    #[cfg(feature = "amqp")]
    {
        r.register_family("amqp", adapter_amqp::factory);
        r.register("rabbitmq+http", adapter_amqp::management_factory);
    }

    #[cfg(feature = "nats")]
    {
        r.register_family("nats", adapter_nats::factory);
    }

    r
}
