pub mod backend;

use dbtool_core::{dsn::Dsn, error::Result, port::connector::Connector};
use futures::future::BoxFuture;

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    backend::connect(dsn)
}
