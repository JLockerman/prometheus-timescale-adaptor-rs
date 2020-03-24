
use chashmap::CHashMap;
use once_cell::sync::Lazy;

use crate::labels::Labels;

static LABELS_CACHE: Lazy<CHashMap<Labels, i64>> = Lazy::new(CHashMap::new);

pub fn get_id(labels: &Labels) -> Option<i64> {
    let guard = LABELS_CACHE.get(labels);
    match guard {
        Some(g) => Some(*g),
        None => None,
    }
}

pub fn set_id(labels: Labels, id: i64) {
    LABELS_CACHE.insert(labels, id);
}

static METRIC_TABLE_CACHE: Lazy<CHashMap<String, &'static str>> = Lazy::new(CHashMap::new);

pub fn get_metric_table_name(name: &str) -> Option<&'static str> {
    let guard = METRIC_TABLE_CACHE.get(name);
    match guard {
        Some(g) => Some(*g),
        None => None,
    }
}

pub fn set_metric_table_name(name: String, table_name: String) -> &'static str {
    let table_name = Box::leak(table_name.into_boxed_str());
    METRIC_TABLE_CACHE.insert(name, table_name);
    table_name
}
