use std::{
    collections::{HashSet, VecDeque},
    hash::Hash,
};

#[derive(Debug)]
pub struct KeyedVecDeque<K, V> {
    vec: VecDeque<(K, V)>,
    index: HashSet<K>,
}

impl<K, V> Default for KeyedVecDeque<K, V> {
    fn default() -> Self {
        Self {
            vec: Default::default(),
            index: Default::default(),
        }
    }
}

impl<K, V> KeyedVecDeque<K, V>
where
    K: Eq + Hash + Clone,
{
    #[allow(dead_code)]
    pub fn new() -> Self {
        KeyedVecDeque {
            vec: VecDeque::new(),
            index: HashSet::new(),
        }
    }

    pub fn push_back(&mut self, key: K, item: V) -> bool {
        if self.index.insert(key.clone()) {
            self.vec.push_back((key, item));
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn push_front(&mut self, key: K, item: V) -> bool {
        if self.index.insert(key.clone()) {
            self.vec.push_front((key, item));
            true
        } else {
            false
        }
    }

    pub fn pop_front(&mut self) -> Option<V> {
        if let Some((k, v)) = self.vec.pop_front() {
            assert!(self.index.remove(&k));
            Some(v)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }
}
