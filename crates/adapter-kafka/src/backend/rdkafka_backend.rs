use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    port::connector::Connector,
};
use futures::future::BoxFuture;

pub fn connect(_dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        Err(Error::Internal(
            "Kafka native backend is not implemented yet; use default backend-pure".into(),
        ))
    })
}
