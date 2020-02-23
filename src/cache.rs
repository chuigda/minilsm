use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead};

use lru::LruCache;
use crate::block::LSMBlockMeta;
use crate::SPLIT_MARK;

pub struct LSMBlockCache {
    data: BTreeMap<String, String>
}

impl LSMBlockCache {
    pub fn new(block_file_meta: LSMBlockMeta) -> Self {
        let block_file_name = block_file_meta.block_file_name();
        let file = File::with_options().read(true).open(block_file_name).unwrap();
        let file = BufReader::new(file);

        let mut ret = LSMBlockCache {
            data: BTreeMap::new()
        };

        for line in file.lines() {
            let line = line.unwrap();
            let parts: Vec<_> = line.split(SPLIT_MARK).collect();
            assert_eq!(parts.len(), 2);
            ret.data.insert(parts[0].to_string(), parts[1].to_string());
        }

        ret
    }

    pub fn lookup(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|s| s.as_str())
    }
}

pub struct LSMCacheManager<'a> {
    lru: LruCache<LSMBlockMeta<'a>, LSMBlockCache>,
    max_cache_count: usize
}

impl<'a> LSMCacheManager<'a> {
    pub fn new(max_cache_count: usize) -> Self {
        LSMCacheManager {
            lru: LruCache::new(max_cache_count),
            max_cache_count
        }
    }

    pub fn get_cache(&mut self, block_meta: LSMBlockMeta<'a>) -> &LSMBlockCache {
        if self.lru.contains(&block_meta) {
            self.lru.get(&block_meta).unwrap()
        } else {
            let new_cache = LSMBlockCache::new(block_meta);
            self.lru.put(block_meta, new_cache);
            self.lru.get(&block_meta).unwrap()
        }
    }

    pub fn max_cache_count(&self) -> usize {
        self.max_cache_count
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_cached_read() {
    }
}