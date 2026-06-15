pub mod alias;
#[allow(clippy::module_inception)]
pub mod registry;

pub use registry::{Factory, Registry};
