use crate::block::{LSMBlock, LSMBlockIter, LSMBlockMeta};
use crate::cache::LSMCacheManager;
use crate::{KVPair, split2, LSMConfig, SPLIT_MARK, DELETION_MARK};

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};

pub(crate) struct FileIdManager {
    current: u32
}

impl FileIdManager {
    fn new(start_with: u32) -> Self {
        FileIdManager { current: start_with }
    }

    fn allocate(&mut self) -> u32 {
        let ret = self.current;
        self.current += 1;
        ret
    }

    fn current(&self) -> u32 {
        self.current
    }
}

impl Default for FileIdManager {
    fn default() -> Self {
        Self::new(0)
    }
}

pub(crate) struct LSMLevel<'a> {
    level: u32,
    blocks: Vec<LSMBlock<'a>>,
    config: &'a LSMConfig,
    file_id_manager: FileIdManager
}

impl<'a> LSMLevel<'a> {
    pub(crate) fn new(level: u32, config: &'a LSMConfig, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks: Vec::new(), config, file_id_manager }
    }

    pub(crate) fn from_meta_file(config: &'a LSMConfig, level: u32) -> Self {
        let level_meta_file = LSMLevel::meta_file_name_int(&config.db_name, level);
        let f = File::with_options().read(true).open(&level_meta_file).unwrap();
        let mut f = BufReader::new(f);

        let mut file_id_line = String::new();
        f.read_line(&mut file_id_line).unwrap();

        let cur_file_id = file_id_line.trim().parse::<u32>().unwrap();
        let file_id_manager = FileIdManager::new(cur_file_id);

        let mut blocks = Vec::new();
        file_id_line.clear();
        while f.read_line(&mut file_id_line).unwrap() != 0 {
            let mut parts: Vec<String> = file_id_line.trim().split(SPLIT_MARK).map(|s| s.to_string()).collect();
            assert_eq!(parts.len(), 4);

            let upper_bound = parts.pop().unwrap();
            let lower_bound = parts.pop().unwrap();
            let block_file_id = parts.pop().unwrap().parse::<u32>().unwrap();
            let origin_level = parts.pop().unwrap().parse::<u32>().unwrap();

            blocks.push(LSMBlock::new(&config.db_name, origin_level, block_file_id, lower_bound, upper_bound));
            file_id_line.clear();
        }

        LSMLevel::with_blocks(level, blocks, config, file_id_manager)
    }

    pub fn update_meta_file(&self) {
        let level_meta_file = self.meta_file_name();
        let mut f = File::with_options().write(true).create(true).truncate(true).open(level_meta_file).unwrap();
        write!(f, "{}\n", self.file_id_manager.current()).unwrap();
        for block in self.blocks.iter() {
            write!(f, "{}{}{}{}{}{}{}\n",
                   block.origin_level, SPLIT_MARK,
                   block.block_file_id, SPLIT_MARK,
                   block.lower_bound(), SPLIT_MARK,
                   block.upper_bound()).unwrap();
        }
    }

    fn meta_file_name(&self) -> String {
        LSMLevel::meta_file_name_int(&self.config.db_name, self.level)
    }

    fn meta_file_name_int(db_name: &str, level: u32) -> String {
        format!("{}_lv{}_meta.mfst", db_name, level)
    }

    fn with_blocks(level: u32, blocks: Vec<LSMBlock<'a>>, config: &'a LSMConfig, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks, config, file_id_manager }
    }

    pub fn get<'b>(&self, key: &str, cache_manager: &'b mut LSMCacheManager<'a>) -> Option<&'b str> {
        if self.level == 1 {
            unsafe {
                let cache_manager = cache_manager as *mut LSMCacheManager;
                for block in self.blocks.iter().rev() {
                    if let Some(value) = block.get(key, cache_manager.as_mut().unwrap()) {
                        return Some(value);
                    }
                }
            }
            None
        } else {
            let mut left: usize = 0;
            let mut right: usize = self.blocks.len();

            while left < right {
                let mid = (left + right) / 2;
                let mid_block = &self.blocks[mid];
                if mid_block.upper_bound() < key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            if right == self.blocks.len() {
                return None;
            }
            self.blocks[right].get(key, cache_manager)
        }
    }

    fn merge_blocks_intern(mut iters: Vec<LSMBlockIter>, level: u32,
                           config: &'a LSMConfig, file_id_manager: &mut FileIdManager) -> Vec<LSMBlock<'a>> {
        #[derive(Eq, PartialEq)]
        struct HeapTriplet(String, String, usize);

        impl Ord for HeapTriplet {
            fn cmp(&self, other: &Self) -> Ordering {
                {
                    let HeapTriplet(key1, _, block_idx1) = self;
                    let HeapTriplet(key2, _, block_idx2) = other;
                    match key1.cmp(key2) {
                        Ordering::Equal => block_idx1.cmp(block_idx2),
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Less => Ordering::Less
                    }
                }.reverse()
            }
        }

        impl PartialOrd for HeapTriplet {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut buffer: Vec<KVPair> = Vec::new();
        let mut blocks_built: Vec<LSMBlock> = Vec::new();
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(KVPair(key, value)) = iter.next() {
                heap.push(HeapTriplet(key, value, i))
            }
        }

        let mut last_block_idx: Option<usize> = None;
        while let Some(HeapTriplet(key, value, block_idx)) = heap.pop() {
            if let Some(KVPair(last_key, _)) = buffer.last() {
                if *last_key == key {
                    let last_block_idx = last_block_idx.unwrap();
                    assert!(block_idx > last_block_idx);
                    buffer.pop();
                    if value.as_str() != DELETION_MARK {
                        buffer.push(KVPair(key, value));
                    }
                } else {
                    if buffer.len() >= config.block_size {
                        blocks_built.push(LSMBlock::create(
                            &config.db_name,
                            level, file_id_manager.allocate(),
                            buffer.drain(0..config.block_size).collect()
                        ));
                    }
                    buffer.push(KVPair(key, value));
                }
            } else {
                buffer.push(KVPair(key, value));
            }

            last_block_idx.replace(block_idx);
            if let Some(KVPair(key, value)) = iters[block_idx].next() {
                heap.push(HeapTriplet(key, value, block_idx));
            }
        }

        if !buffer.is_empty() {
            blocks_built.push(LSMBlock::create(
                &config.db_name,
                level, file_id_manager.allocate(),
                buffer
            ));
        }

        blocks_built
    }

    pub fn create_block(&mut self, block_data: Vec<KVPair>) -> bool {
        assert_eq!(self.level, 1);

        let new_block = LSMBlock::create(&self.config.db_name, 1,
                                         self.file_id_manager.allocate(), block_data);
        self.blocks.push(new_block);
        self.blocks.len() > self.level_size_max()
    }

    fn overlapping_lower_bound(range: (&str, &str), blocks: &Vec<LSMBlock>) -> usize {
        let (low, _) = range;
        let mut left = 0;
        let mut right = blocks.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_block = &blocks[mid];
            if mid_block.lower_bound() > low {
                right = mid
            } else if mid_block.upper_bound() < low {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        left
    }

    fn overlapping_upper_bound(range: (&str, &str), blocks: &Vec<LSMBlock>) -> usize {
        let (_, high) = range;
        let mut left = 0;
        let mut right = blocks.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_block = &blocks[mid];
            if mid_block.lower_bound() > high {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        right
    }

    pub fn merge_blocks(&mut self, mut blocks: Vec<LSMBlock<'a>>) -> (Vec<LSMBlockMeta<'a>>, bool) {
        assert_ne!(self.level, 1);

        let mut self_blocks = Vec::new();
        self_blocks.append(&mut self.blocks);

        eprintln!("DBG_LOG: merging {:?} at level {} into {:?}", blocks, self.level - 1, self_blocks);

        let (self_to_merge, self_stand_still) = if self.level == 2 {
            (self_blocks, Vec::new())
        } else {
            let input_range = (blocks.first().unwrap().lower_bound(), blocks.last().unwrap().upper_bound());
            let lower_idx = LSMLevel::overlapping_lower_bound(input_range, &self_blocks);
            let upper_idx = LSMLevel::overlapping_upper_bound(input_range, &self_blocks);

            (self_blocks.drain(lower_idx..upper_idx).collect(), self_blocks)
        };
        eprintln!("DBG_LOG: will participate in merging: {:?}", self_to_merge);
        eprintln!("DBG_LOG: will stand still: {:?}", self_stand_still);

        let removed_files =
            self_to_merge
                .iter()
                .chain(blocks.iter())
                .map(|block| block.metadata())
                .collect::<Vec<_>>();

        let merging_iters =
            self_to_merge
                .iter()
                .chain(blocks.iter())
                .map(|block| block.iter())
                .collect::<Vec<_>>();

        let mut new_blocks =
            LSMLevel::merge_blocks_intern(merging_iters, self.level,
                                          self.config, &mut self.file_id_manager);
        eprintln!("DBG_LOG: newly produced blocks are {:?}\n", new_blocks);


        let mut all_blocks = self_stand_still;
        all_blocks.append(&mut new_blocks);

        self.blocks.append(&mut all_blocks);
        self.blocks.sort_by(|block1, block2| block1.lower_bound().cmp(block2.lower_bound()));

        let b = self.blocks.len() > self.level_size_max();
        (removed_files, b)
    }

    fn level_size_max(&self) -> usize {
        self.config.level_size_max(self.level as usize)
    }

    pub fn blocks_to_merge(&mut self) -> Vec<LSMBlock<'a>> {
        assert!(self.blocks.len() > self.level_size_max());

        if self.level == 1 {
            let mut ret = Vec::new();
            ret.append(&mut self.blocks);
            ret
        } else {
            let n = self.blocks.len();
            self.blocks.drain(n-self.config.merge_step_size..n).collect()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::block::{LSMBlock, LSMBlockMeta};
    use crate::level::{LSMLevel, FileIdManager};
    use crate::cache::LSMCacheManager;
    use crate::{KVPair, LSMConfig, DELETION_MARK};

    #[test]
    fn test_level1_lookup() {
        let data_pieces = vec![
            vec![
                ("ice1000", "100"),
                ("xyy", "99"),
                ("chu1gda", "60"),
                ("lyzh", "60/182.5")
            ],
            vec![
                ("ice1000", "101"),
                ("fish", "55"),
                ("xtl", "45"),
                ("duangsuse", "0")
            ],
            vec![
                ("chu1gda", "75"),
                ("duangsuse", "1"),
                ("xtl", "55"),
                ("huge", "95")
            ],
            vec![
                ("fzy", "100"),
                ("zyf", "100"),
                ("lyzh", "75"),
                ("ice1000", "1000")
            ]
        ];
        let blocks =
            data_pieces.into_iter().enumerate().map(|(i, vec)| {
                let mut data =
                    vec.into_iter()
                       .map(|(k, v)| KVPair(k.to_string(), v.to_string()))
                       .collect::<Vec<_>>();
                data.sort();
                LSMBlock::create("tdb1", 1, i as u32, data)
            }).collect::<Vec<_>>();
        let lsm_config = LSMConfig::testing("tdb1");
        let file_id_manager = FileIdManager::default();
        let mut cache_manager = LSMCacheManager::new(4);
        let level = LSMLevel::with_blocks(1, blocks, &lsm_config, file_id_manager);

        assert_eq!(level.get("lyzh", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("chu1gda", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("xtl", &mut cache_manager).unwrap(), "55");
        assert_eq!(level.get("huge", &mut cache_manager).unwrap(), "95");
        assert_eq!(level.get("xyy", &mut cache_manager).unwrap(), "99");
        assert_eq!(level.get("zyf", &mut cache_manager).unwrap(), "100");
        assert_eq!(level.get("fzy", &mut cache_manager).unwrap(), "100");
        assert!(level.get("zyy", &mut cache_manager).is_none());
    }

    use crate::kv_pair;
    use crate::test_util::gen_kv;
    use std::collections::BTreeMap;

    #[test]
    fn test_lvn_lookup() {
        let lsm_config = LSMConfig::testing("tdb2");
        let file_id_manager = FileIdManager::default();
        let mut cache_manager = LSMCacheManager::new(4);

        let kvs = vec![
            gen_kv("aaa", lsm_config.block_size),
            gen_kv("aba", lsm_config.block_size),
            gen_kv("aca", lsm_config.block_size),
            gen_kv("ada", lsm_config.block_size)
        ];
        let blocks =
            kvs.clone()
                .into_iter()
                .enumerate()
                .map(|(i, data)| {
                    LSMBlock::create("tdb2", 2, i as u32, data)
                })
                .collect();
        let level2 = LSMLevel::with_blocks(2, blocks, &lsm_config, file_id_manager);

        for kv_piece in &kvs {
            for KVPair(key, value) in kv_piece {
                assert_eq!(level2.get(key, &mut cache_manager).unwrap(), value);
            }
        }
    }

    #[test]
    fn test_merge() {
        // Input blocks
        // LV1       [AAE ~ AAL]                   [ACA-ACH]
        // LV2 [AAA ~ AAJ] [AAK ~ AAT] [ABA ~ ABH]           [ADA ~ ADH]
        //
        // expected output blocks, blocks marked '*' are newly created
        // LV2 *[AAA ~ AAH] *[AAI ~ AAP] *[AAQ ~ AAT] [ABA ~ ABH] [ACA ~ ACH] [ADA ~ ADH]

        let lsm_config = LSMConfig::testing("tdb3");

        // We start from a large file id so that it does not over write our existing blocks
        let lv2_file_id_manager = FileIdManager::new(99);
        let mut cache_manager = LSMCacheManager::new(4);

        let lv1_data = vec![
            gen_kv("aae", 8),
            gen_kv("aca", 8)
        ];
        let lv2_data = vec![
            // the first two blocks must be built manually
            vec![
                kv_pair!("aaa", "unique1"),
                kv_pair!("aab", "unique2"),
                kv_pair!("aac", "unique3"),
                kv_pair!("aad", "unique4"),
                kv_pair!("aae", "special1"),
                kv_pair!("aag", "special2"),
                kv_pair!("aah", "special3"),
                kv_pair!("aaj", "special4"),
            ],
            vec![
                kv_pair!("aal", "很多时候"),
                kv_pair!("aam", "重复地向别人请教一些高度相似的问题"),
                kv_pair!("aan", "尤其是在这个问题很trivial的情况下"),
                kv_pair!("aao", "是一种很不礼貌的行为"),
                kv_pair!("aaq", "最近在网上看到一个人"),
                kv_pair!("aar", "他每天都会重复一件事"),
                kv_pair!("aas", "首先问一个用直觉就能感觉出来答案的问题"),
                kv_pair!("aat", "然后在刚才的问题中的名词随便选一个"),
            ],
            gen_kv("aba", 8),
            gen_kv("ada", 8)
        ];

        let mut expectations = BTreeMap::new();
        lv2_data.iter().chain(lv1_data.iter()).for_each(|kvs| {
            kvs.iter().for_each(|KVPair(k, v)| {
                let _ = expectations.insert(k, v);
            })
        });

        let lv1_blocks =
            lv1_data
                .clone().into_iter()
                .enumerate()
                .map(|(i, kvs)| {
                    LSMBlock::create("tdb3", 1, i as u32, kvs)
                })
                .collect::<Vec<_>>();

        let lv2_blocks =
            lv2_data
                .clone().into_iter()
                .enumerate()
                .map(|(i, kvs)| {
                    LSMBlock::create("tdb3", 2, i as u32, kvs)
                })
                .collect::<Vec<_>>();

        let mut level2 = LSMLevel::with_blocks(2, lv2_blocks, &lsm_config, lv2_file_id_manager);
        let _ = level2.merge_blocks(lv1_blocks);

        for (&k, &v) in expectations.iter() {
            assert_eq!(level2.get(k, &mut cache_manager).unwrap(), v)
        }

        for (i, b1) in level2.blocks.iter().enumerate() {
            for (j, b2) in level2.blocks.iter().enumerate() {
                if i != j {
                    assert!(!LSMBlock::interleave(b1, b2))
                }
            }
        }
    }

    #[test]
    fn test_merge2() {
        // LV1 [AAA ~ AAH]      [AAQ ~ AAX]
        //          [AAE ~ AAL]
        // LV2 (Empty)
        //
        // expected output blocks, blocks marked '*' are newly created
        // LV2 *[AAA ~ AAH] *[AAI ~ AAL] [AAQ ~ AAX]

        let lsm_config = LSMConfig::testing("tdb4");

        // We start from a large file id so that it does not over write our existing blocks
        let mut cache_manager = LSMCacheManager::new(4);

        let lv1_data = vec![
            gen_kv("aaa", 8),
            gen_kv("aae", 8),
            gen_kv("aaq", 8)
        ];

        let mut expectations = BTreeMap::new();
        lv1_data.iter().for_each(|kvs| {
            kvs.iter().for_each(|KVPair(k, v)| {
                let _ = expectations.insert(k, v);
            })
        });

        let lv1_blocks =
            lv1_data
                .clone().into_iter()
                .enumerate()
                .map(|(i, kvs)| {
                    LSMBlock::create("tdb4", 1, i as u32, kvs)
                })
                .collect::<Vec<_>>();

        let mut level2 = LSMLevel::new(2, &lsm_config, FileIdManager::default());
        let _ = level2.merge_blocks(lv1_blocks);

        for (&k, &v) in expectations.iter() {
            assert_eq!(level2.get(k, &mut cache_manager).unwrap(), v)
        }

        for (i, b1) in level2.blocks.iter().enumerate() {
            for (j, b2) in level2.blocks.iter().enumerate() {
                if i != j {
                    assert!(!LSMBlock::interleave(b1, b2))
                }
            }
        }
    }

    #[test]
    fn test_merge_with_deletion() {
        let lv1_block = LSMBlock::create("tdb5", 1, 1, vec![
            kv_pair!("AAB", "人在美国"),
            kv_pair!("AAC", DELETION_MARK),
            kv_pair!("AAD", DELETION_MARK),
            kv_pair!("AAF", DELETION_MARK),
            kv_pair!("AAG", "刚下飞机"),
            kv_pair!("AAH", "利益相关"),
            kv_pair!("AAI", DELETION_MARK),
            kv_pair!("AAJ", "手机怒答"),
        ]);

        let lv2_blocks = vec![
            LSMBlock::create("tdb5", 2, 1, vec![
                kv_pair!("AAA", "我们一再强调：你提到的有关媒体报道的内容有误，完全是别有用心"),
                kv_pair!("AAB", "对於锤哥一再挑动开发数据库，我只能用一句古诗形容锤哥开发数据库的後果"),
                kv_pair!("AAC", "「风萧萧兮易水寒，壮士一去兮不复还！」"),
                kv_pair!("AAD", "锤哥开发数据库必定面对世人的侧目"),
                kv_pair!("AAE", "锤哥不仅不以为耻反以为荣"),
                kv_pair!("AAF", "「明明已经从山巅之城跌落，还浑然不觉，颐指气使」"),
                kv_pair!("AAG", "这样的锤哥，能和中国相提并论吗！"),
                kv_pair!("AAH", "对于一些触目惊心的事实，锤哥难道真的不知道吗？")
            ]),
            LSMBlock::create("tdb5", 2, 2, vec![
                kv_pair!("AAI", "中国政府和中国人民坚决反对锤哥开发数据库的行为"),
                kv_pair!("AAJ", "中方已就此向锤哥提出严正交涉和强烈抗议"),
                kv_pair!("AAK", "中国一向秉持和平共处五项原则处理国与国关系"),
                kv_pair!("AAL", "历来坚定奉行不干涉内政原则"),
                kv_pair!("AAM", "主张各国根据自身国情选择发展道路"),
                kv_pair!("AAN", "希望锤哥能以开放的心态和客观公正的态度对待中国"),
                kv_pair!("AAO", "不要无中生有"),
                kv_pair!("AAP", "锤哥罔顾事实、混淆是非、违反公理，玩弄双重标准")
            ])
        ];

        let lsm_config = LSMConfig::testing("tdb5");
        let mut cache_manager = LSMCacheManager::new(4);
        let mut level2 = LSMLevel::with_blocks(2, lv2_blocks, &lsm_config, FileIdManager::new(32));
        let _ = level2.merge_blocks(vec![lv1_block]);

        assert!(level2.get("AAC", &mut cache_manager).is_none());
        assert!(level2.get("AAD", &mut cache_manager).is_none());
        assert!(level2.get("AAF", &mut cache_manager).is_none());
        assert!(level2.get("AAI", &mut cache_manager).is_none());
        assert_eq!(level2.get("AAB", &mut cache_manager).unwrap(), "人在美国");
        assert_eq!(level2.get("AAG", &mut cache_manager).unwrap(), "刚下飞机");
        assert_eq!(level2.get("AAH", &mut cache_manager).unwrap(), "利益相关");
        assert_eq!(level2.get("AAJ", &mut cache_manager).unwrap(), "手机怒答");
    }

    #[test]
    fn test_issue_24() {
        let lsm_config =
            LSMConfig::new("test_issue24",
                           1, 2, 2,
                           4, 2, 2);
        let mut file_id_manager = FileIdManager::new(10);

        let lv3_blocks = vec![
            vec![
                kv_pair!("AAA", "比赛爬山"),
                kv_pair!("AAB", "比赛拔河"),
                kv_pair!("AAW", "我经济上不如你们"),
                kv_pair!("AAX", "我身体比你们棒"),
            ],
            vec![
                kv_pair!("ABA", "打黑除恶"),
                kv_pair!("ABB", "我有思想准备"),
                kv_pair!("ABC", "是要触及一些人的利益"),
                kv_pair!("ABD", "也有一些人"),
            ],
            vec![
                kv_pair!("ABE", "给我泼脏水"),
                kv_pair!("ABF", "说我儿子在外面开红色法拉利"),
                kv_pair!("ABG", "一派胡言"),
                kv_pair!("ABH", "我和我妻子没有任何个人财产"),
            ]
        ].into_iter().enumerate().map(|(i, kvs)| {
            LSMBlock::create("test_issue24", 3, i as u32, kvs)
        }).collect::<Vec<_>>();
        let mut lv3 = LSMLevel::with_blocks(3, lv3_blocks, &lsm_config, file_id_manager);

        let lv2_blocks = vec![
            vec![
                kv_pair!("AAW", "几十年就是这样下来了"),
                kv_pair!("AAX", "我妻子是律师事务所有名的律师"),
                kv_pair!("AAY", "在大连期间搞律师事务所就很成功"),
                kv_pair!("AAZ", "北大教授王铁涯那是她的老师")
            ],
            vec![
                kv_pair!("ABE", "我不是新闻工作者"),
                kv_pair!("ABF", "但是我见得多了"),
                kv_pair!("ABX", "西方发达国家有哪个我没去过"),
                kv_pair!("ABY", "美国的华莱士比你们不知高到哪里去了")
            ]
        ].into_iter().enumerate().map(|(i, kvs)| {
            LSMBlock::create("test_issue24", 2, i as u32, kvs)
        }).collect::<Vec<_>>();

        lv3.merge_blocks(lv2_blocks);
        lv3.update_meta_file();
        for (i, b1) in lv3.blocks.iter().enumerate() {
            for (j, b2) in lv3.blocks.iter().enumerate() {
                if i != j {
                    assert!(!LSMBlock::interleave(b1, b2));
                }
            }
        }
    }
}
