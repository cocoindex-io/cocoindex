use rustc_hash::FxHashMap;
use std::any::{Any, TypeId};

/// Type-erased map where the type IS the key. Like `http::Extensions`.
pub(crate) struct TypeMap(FxHashMap<TypeId, Box<dyn Any + Send + Sync>>);

impl TypeMap {
    pub fn new() -> Self {
        Self(FxHashMap::default())
    }

    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.0.contains_key(&TypeId::of::<T>())
    }

    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.0.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get() {
        let mut map = TypeMap::new();
        map.insert(42u32);
        map.insert("hello".to_string());

        assert_eq!(map.get::<u32>(), Some(&42));
        assert_eq!(map.get::<String>(), Some(&"hello".to_string()));
        assert_eq!(map.get::<i64>(), None);
    }

    #[test]
    fn overwrite() {
        let mut map = TypeMap::new();
        map.insert(1u32);
        map.insert(2u32);
        assert_eq!(map.get::<u32>(), Some(&2));
    }

    #[test]
    fn contains_type() {
        let mut map = TypeMap::new();
        assert!(!map.contains::<u32>());
        map.insert(1u32);
        assert!(map.contains::<u32>());
    }
}
