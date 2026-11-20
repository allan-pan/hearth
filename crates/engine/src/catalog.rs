use std::collections::HashMap;
use storage::table::Snapshot;
pub trait Catalog: Send + Sync {
    fn table(&self, name: &str) -> Option<Snapshot>;
}

pub struct MemoryCatalog {
    tables: HashMap<String, Snapshot>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, snapshot: Snapshot) {
        self.tables.insert(name.to_string(), snapshot);
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog for MemoryCatalog {
    fn table(&self, name: &str) -> Option<Snapshot> {
        self.tables.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn registered_table_found() {
        let mut catalog = MemoryCatalog::new();
        let snapshot = Snapshot {
            version: 7,
            files: vec![],
            schema: Arc::new(Schema::empty()),
        };

        catalog.register("test_table", snapshot);

        let retrieved_snapshot = catalog.table("test_table");
        assert_eq!(retrieved_snapshot.unwrap().version, 7);
    }

    #[test]
    fn missing_table_returns_none() {
        let catalog = MemoryCatalog::new();
        let retrieved_snapshot = catalog.table("non_existent_table");
        assert!(retrieved_snapshot.is_none());
    }
}
