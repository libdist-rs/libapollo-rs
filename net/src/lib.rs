pub mod futures_manager;
pub mod tokio_manager;

/// Trait bound for types that travel through the vendored net codec.
/// Rely on raw serde + the standard thread-safety markers. Callers
/// implement `Message` (from `net_common`) for protocol-level decode
/// fallibility; inside the vendored net we just need a blanket bound.
pub trait NetMsg:
    serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static
{
}
impl<T> NetMsg for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static
{
}