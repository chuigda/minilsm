#![feature(with_options)]
#![feature(drain_filter)]

mod cache;
mod block;
mod level;
mod test_util;

use cache::*;
use block::*;
use level::*;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::fs::File;
use std::io::{Write, Read};
use std::fs;

const DELETION_MARK: &'static str = "_";
const SPLIT_MARK: &'static str = ":";

#[derive(Debug, Clone)]
pub struct KVPair(String, String);

impl PartialEq for KVPair {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for KVPair {}

impl Ord for KVPair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for KVPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn split2<T, F>(mut v: Vec<T>, f: F) -> (Vec<T>, Vec<T>)
    where F: Fn(&mut T) -> bool {
    let v1 = v.drain_filter(f).collect::<Vec<_>>();
    (v1, v)
}

#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub db_name: String,
    pub level1_size: usize,
    pub level2_size: usize,
    pub size_scale: usize,
    pub block_size: usize,
    pub merge_step_size: usize,
    pub max_cache_size: usize
}

impl LSMConfig {
    fn new(db_name: impl ToString,
           level1_size: usize,
           level2_size: usize,
           size_scale: usize,
           block_size: usize,
           merge_step_size: usize,
           max_cache_size: usize) -> Self {
        assert!(size_scale >= 2);
        assert!(merge_step_size <= level2_size);
        assert!(merge_step_size > level1_size);
        LSMConfig {
            db_name: db_name.to_string(),
            level1_size,
            level2_size,
            size_scale,
            block_size,
            merge_step_size,
            max_cache_size
        }
    }

    fn testing(db_name: impl ToString) -> Self {
        // WARNING: Do NOT change these parameters. Changing these parameters requires changes on tests. see level.rs
        // for further details.
        LSMConfig::new(db_name, 2, 4, 2, 8, 4, 16)
    }

    fn level_size_max(&self, level: usize) -> usize {
        if level == 1 {
            self.level1_size
        } else {
            self.level2_size * self.size_scale.pow((level - 1) as u32)
        }
    }
}

impl Default for LSMConfig {
    fn default() -> Self {
        LSMConfig::new("db1", 4, 10, 10, 1024, 6, 16)
    }
}

struct LSM<'a> {
    config: LSMConfig,
    cache_manager: RefCell<LSMCacheManager<'a>>,
    mut_table: BTreeMap<String, String>,
    levels: Vec<LSMLevel<'a>>
}

impl Drop for LSM<'_> {
    fn drop(&mut self) {
        self.update_manifest();
        if self.mut_table.len() == 0 {
            return;
        }

        LSMBlock::create(&self.config.db_name, 0, 0,
                         self.mut_table.iter().map(|(k, v)| {
                             KVPair(k.to_string(), v.to_string())
                         }).collect());
    }
}

impl<'a> LSM<'a> {
    pub fn new(config: LSMConfig) -> Self {
        let cache_manager = LSMCacheManager::new(config.max_cache_size);
        let ret = LSM {
            config,
            cache_manager: RefCell::new(cache_manager),
            mut_table: BTreeMap::new(),
            levels: Vec::new()
        };
        ret
    }

    pub fn open(config: LSMConfig) -> Self {
        let mut ret = LSM::new(config);
        let manifest_file_name = ret.manifest_file_name();
        let mut f = File::with_options().read(true).open(manifest_file_name).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();
        let levels = buf.trim().parse::<u32>().unwrap();
        for i in 1..=levels {
            ret.levels.push(LSMLevel::from_meta_file(ret.self_config(), i));
        }

        if !fs::read_dir(".")
              .unwrap()
              .any(|d| {
                  d.unwrap().file_name().to_str().unwrap()
                  == LSMBlockMeta::new(&ret.config.db_name, 0, 0).block_file_name().as_str()
              }) {
            return ret;
        }

        let mut cache_block_iter =
            LSMBlockIter::new(
                LSMBlockMeta::new(&ret.config.db_name, 0, 0));
        while let Some(KVPair(k, v)) = cache_block_iter.next() {
            ret.mut_table.insert(k, v);
        }

        ret
    }

    pub fn self_config(&self) -> &'a LSMConfig {
        unsafe {
            (&self.config as *const LSMConfig).as_ref().unwrap()
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(ret) = self.mut_table.get(key) {
            return if ret == DELETION_MARK {
                None
            } else {
                Some(ret.to_string())
            }
        }
        for level in self.levels.iter() {
            if let Some(ret) = level.get(key, self.cache_manager.borrow_mut().deref_mut()) {
                return if ret == DELETION_MARK {
                    None
                } else {
                    Some(ret.to_string())
                }
            }
        }
        None
    }

    pub fn put(&mut self, key: impl ToString, value: impl ToString) {
        let _ = self.mut_table.insert(key.to_string(), value.to_string());
        if self.mut_table.len() <= self.config.block_size {
            return;
        }

        let mut block_data = BTreeMap::new();
        block_data.append(&mut self.mut_table);
        let block_data =
            block_data.into_iter().map(|(k, v)| KVPair(k, v)).collect::<Vec<_>>();

        if self.levels.len() == 0 {
            self.levels.push(LSMLevel::new(1, self.self_config(),
                                           FileIdManager::default()))
        }

        let mut require_merge = self.levels[0].create_block(block_data);
        self.levels[0].update_meta_file();

        let mut next_level_idx = 1;
        while require_merge {
            if self.levels.len() <= next_level_idx {
                self.levels.push(LSMLevel::new(next_level_idx as u32 + 1, self.self_config(),
                                               FileIdManager::default()));
            }
            let blocks_to_merge = self.levels[next_level_idx - 1].blocks_to_merge();
            let (removed_files, require_merge_next) =
                self.levels[next_level_idx].merge_blocks(blocks_to_merge);
            self.levels[next_level_idx - 1].update_meta_file();
            self.levels[next_level_idx].update_meta_file();

            LSM::remove_files(removed_files);
            require_merge = require_merge_next;
            next_level_idx += 1;
        }

        self.update_manifest();
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(value) = self.get(key) {
            self.put(key, DELETION_MARK);
            return true;
        } else {
            return false;
        }
    }

    fn remove_files(files: Vec<LSMBlockMeta<'a>>) {
        for file_meta in files {
            std::fs::remove_file(file_meta.block_file_name()).unwrap();
        }
    }

    fn update_manifest(&self) {
        let mut f =
            File::with_options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(self.manifest_file_name()).unwrap();
        write!(f, "{}", self.levels.len()).unwrap();
    }

    fn manifest_file_name(&self) -> String {
        format!("{}_main.mfst", self.config.db_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::{LSMConfig, LSM, KVPair};
    use crate::test_util::{gen_kv, FakeRng};
    use rand::prelude::{ThreadRng, SliceRandom};
    use std::collections::BTreeMap;

    #[test]
    fn workload_test() {
        let mut kvs = gen_kv("aaa", 512);
        let mut rng = FakeRng::default();
        kvs.shuffle(&mut rng);

        let lsm_config = LSMConfig::testing("wl_test_db");
        let mut lsm = LSM::new(lsm_config);
        let mut memds = BTreeMap::new();

        for KVPair(k, v) in kvs.iter() {
            lsm.put(k, v);
            memds.insert(k, v);
        }

        for (&k, &v) in memds.iter() {
            eprintln!("DBG_LOG: expecting {} -> {}", k, v);
            assert_eq!(lsm.get(k).unwrap(), *v);
        }
    }

    #[test]
    fn deletion_test() {
        let mut kvs = gen_kv("aaa", 512);
        let mut rng = FakeRng::default();
        kvs.shuffle(&mut rng);

        let lsm_config = LSMConfig::testing("dl_test_db");
        let mut lsm = LSM::new(lsm_config);
        let mut memds = BTreeMap::new();
        for KVPair(k, v) in kvs.iter() {
            lsm.put(k, v);
            memds.insert(k, v);
        }

        let mut kvs_1 = gen_kv("aaa", 512);
        kvs_1.shuffle(&mut rng);
        let mut removed = Vec::new();
        for KVPair(k, v) in kvs_1.iter().take(128) {
            assert!(lsm.delete(k));
            memds.remove(k);

            removed.push(k.to_string());
        }

        for (&k, &v) in memds.iter() {
            eprintln!("DBG_LOG: expecting {} -> {}", k, v);
            assert_eq!(lsm.get(k).unwrap(), *v);
        }

        for k in removed.iter() {
            assert!(lsm.get(k).is_none())
        }
    }

    #[test]
    fn overlapping_test() {
        let mut kvs = Vec::new();
        for _ in 0..8 {
            kvs.append(&mut gen_kv("aaa", 57));
        }

        let mut rng = ThreadRng::default();
        kvs.shuffle(&mut rng);

        let lsm_config = LSMConfig::testing("ol_test_db");
        let mut lsm = LSM::new(lsm_config);
        let mut memds = BTreeMap::new();

        for KVPair(k, v) in kvs.iter() {
            lsm.put(k, v);
            memds.insert(k, v);
        }

        for (&k, &v) in memds.iter() {
            assert_eq!(lsm.get(k).unwrap(), *v);
        }
    }

    #[test]
    fn reopen_test() {
        let mut kvs = Vec::new();
        kvs.append(&mut gen_kv("aaa", 406));
        kvs.append(&mut gen_kv("aja", 356));
        let mut rng = ThreadRng::default();
        kvs.shuffle(&mut rng);

        let lsm_config = LSMConfig::testing("rop_test_db");

        let mut memds = BTreeMap::new();

        {
            let mut lsm = LSM::new(lsm_config.clone());
            for KVPair(k, v) in kvs.iter() {
                lsm.put(k, v);
                memds.insert(k, v);
            }

            std::mem::drop(lsm);
        }

        let lsm = LSM::open(lsm_config);
        for (&k, &v) in memds.iter() {
            eprintln!("expected kv-pair: {} -> {}", k, v);
            assert_eq!(lsm.get(k).unwrap(), *v);
        }
    }

    #[test]
    fn reopen_empty() {
        let lsm_config = LSMConfig::testing("rop_empty_db");
        let lsm = LSM::new(lsm_config.clone());
        std::mem::drop(lsm);

        let lsm = LSM::open(lsm_config);
        assert_eq!(2 + 2, 4);
    }
}
