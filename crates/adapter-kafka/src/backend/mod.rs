use dbtool_core::{dsn::Dsn, error::Result, port::connector::Connector};
use futures::future::BoxFuture;

// Mutually exclusive backend selection (§12.2).
#[cfg(feature = "backend-native")]
mod rdkafka_backend;
#[cfg(not(feature = "backend-native"))]
mod rskafka_backend;

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    #[cfg(feature = "backend-native")]
    {
        rdkafka_backend::connect(dsn)
    }
    #[cfg(not(feature = "backend-native"))]
    {
        rskafka_backend::connect(dsn)
    }
}
