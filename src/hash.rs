use ahash::RandomState;

#[allow(clippy::module_name_repetitions)]
pub type HashMap<K, V> = std::collections::HashMap<K, V, RandomState>;
#[allow(clippy::module_name_repetitions)]
pub type HashSet<V> = std::collections::HashSet<V, RandomState>;
pub type DashMap<K, V> = dashmap::DashMap<K, V, RandomState>;
pub type DashSet<V> = dashmap::DashSet<V, RandomState>;
