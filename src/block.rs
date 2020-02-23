use std::io::{BufReader, BufRead, Write};
use std::fs::File;
use std::iter::FusedIterator;

use crate::cache::LSMCacheManager;
use crate::{KVPair, SPLIT_MARK};
use std::fmt::{Debug, Formatter, Error};

#[derive(Clone)]
pub struct LSMBlock<'a> {
    db_name: &'a str,
    pub(crate) origin_level: u32,
    pub(crate) block_file_id: u32,
    lower_bound: String,
    upper_bound: String
}

impl<'a> Debug for LSMBlock<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}@[{}, {}]", self.block_file_name(), self.lower_bound, self.upper_bound)
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct LSMBlockMeta<'a> {
    db_name: &'a str,
    origin_level: u32,
    block_file_id: u32
}

impl<'a> Debug for LSMBlockMeta<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.block_file_name())
    }
}

impl<'a> LSMBlockMeta<'a> {
    pub(crate) fn new(db_name: &'a str, origin_level: u32, block_file_id: u32) -> Self {
        LSMBlockMeta { db_name, origin_level, block_file_id }
    }
}

impl LSMBlockMeta<'_> {
    pub fn block_file_name(&self) -> String {
        LSMBlock::block_file_name_int(self.db_name, self.origin_level, self.block_file_id)
    }
}

pub struct LSMBlockIter {
    block_file_handle: BufReader<File>
}

impl LSMBlockIter {
    pub fn new(block_file_meta: LSMBlockMeta) -> Self {
        LSMBlockIter {
            block_file_handle:
                BufReader::new(
                    File::with_options().read(true).open(block_file_meta.block_file_name()).unwrap())
        }
    }
}

impl Iterator for LSMBlockIter {
    type Item = KVPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = String::new();
        let bytes_read = self.block_file_handle.read_line(&mut buf).unwrap();
        if bytes_read == 0 {
            None
        } else {
            let parts: Vec<_> = buf.trim().split(SPLIT_MARK).collect();
            assert_eq!(parts.len(), 2);
            Some(KVPair(parts[0].to_string(), parts[1].to_string()))
        }
    }
}

impl FusedIterator for LSMBlockIter { }

impl<'a> LSMBlock<'a> {
    pub fn new(db_name: &'a str,
               origin_level: u32,
               block_file_id: u32,
               lower_bound: String,
               upper_bound: String) -> Self {
        LSMBlock {
            db_name, origin_level, block_file_id, lower_bound, upper_bound
        }
    }

    pub fn create(db_name: &'a str, origin_level: u32, block_file_id: u32, data: Vec<KVPair>) -> Self {
        let lower_bound = data.first().unwrap().0.clone();
        let upper_bound = data.last().unwrap().0.clone();

        let block_file_name = LSMBlock::block_file_name_int(db_name, origin_level, block_file_id);
        let mut file =
            File::with_options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&block_file_name).unwrap();
        for data_line in data {
            write!(file, "{}{}{}\n", data_line.0, SPLIT_MARK, data_line.1).unwrap();
        }

        LSMBlock::new(db_name, origin_level, block_file_id, lower_bound, upper_bound)
    }

    pub fn block_file_name(&self) -> String {
        LSMBlock::block_file_name_int(self.db_name, self.origin_level, self.block_file_id)
    }

    pub fn metadata(&self) -> LSMBlockMeta<'a> {
        LSMBlockMeta { db_name: self.db_name, origin_level: self.origin_level, block_file_id: self.block_file_id }
    }

    fn block_file_name_int(db_name: &str, origin_level: u32, block_file_id: u32) -> String {
        format!("{}_lv{}_{}.msst", db_name, origin_level, block_file_id)
    }

    pub fn lower_bound(&self) -> &str {
        self.lower_bound.as_str()
    }

    pub fn upper_bound(&self) -> &str {
        self.upper_bound.as_str()
    }

    pub fn get<'b>(&self, key: &str, cache_manager: &'b mut LSMCacheManager<'a>) -> Option<&'b str> {
        if key >= self.lower_bound.as_str() && key <= self.upper_bound.as_str() {
            cache_manager.get_cache(self.metadata()).lookup(key)
        } else {
            None
        }
    }

    pub fn iter(&self) -> LSMBlockIter {
        LSMBlockIter::new(self.metadata())
    }

    pub fn interleave(b1: &LSMBlock, b2: &LSMBlock) -> bool {
        (b1.lower_bound <= b2.upper_bound && b2.lower_bound <= b1.upper_bound)    // intersect
        || (b1.lower_bound <= b2.lower_bound && b1.upper_bound >= b2.upper_bound) // b1 includes b2
        || (b2.lower_bound <= b1.lower_bound && b2.upper_bound >= b1.upper_bound) // b2 includes b1
    }
}

#[cfg(test)]
mod test {
    use crate::block::LSMBlock;
    use crate::cache::LSMCacheManager;
    use crate::KVPair;

    #[test]
    fn test_build_sst() {
        let mut data = vec![
            ("Yifan ZHU", "CHUK(SZ)"),
            ("ice1000", "PSU"),
            ("ICEY", "QDU"),
            ("Lyzh", "Railway Middle School"),
            ("Accelerator", "長期コンピューター学校"),
            ("MisakawaMikoto", "常盤台中学校"),
            ("KamijouTouma", "PERMISSION_DENIED")
        ];
        data.sort_by(|&(k1, _), &(k2, _)| k1.cmp(k2));
        let data =
            data.iter()
                .map(|&(key, value)| KVPair(key.to_string(), value.to_string()))
                .collect::<Vec<_>>();

        let block = LSMBlock::create("tdb0", 0, 0, data);
        let mut cache_manager = LSMCacheManager::new(10);

        assert_eq!(block.get("ice1000", &mut cache_manager).unwrap(), "PSU");
    }

    #[test]
    fn test_block_iter() {
        let mut data = vec![
            ("Yifan ZHU", "CHUK(SZ)"),
            ("ice1000", "PSU"),
            ("ICEY", "QDU"),
            ("Lyzh", "Railway Middle School"),
            ("Accelerator", "長期コンピューター学校"),
            ("MisakawaMikoto", "常盤台中学校"),
            ("KamijouTouma", "PERMISSION_DENIED")
        ];
        let mut data =
            data.iter()
                .map(|&(key, value)| KVPair(key.to_string(), value.to_string()))
                .collect::<Vec<_>>();
        data.sort();
        let block = LSMBlock::create("tdb1", 0, 0, data.clone());
        let block_iter = block.iter();

        let iter_read_data = block_iter.collect::<Vec<_>>();
        assert_eq!(iter_read_data, data);
    }
}
