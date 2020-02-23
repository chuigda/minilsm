use lazy_static::lazy_static;
use std::ops::Deref;
use rand::{Rng, RngCore, Error};
use rand::rngs::ThreadRng;
use crate::KVPair;
use std::thread::Thread;

#[macro_export]
macro_rules! kv_pair {
    ($k:expr, $v:expr) => (KVPair($k.to_owned(), $v.to_owned()));
}

pub(crate) struct FakeRng {
    current: u64
}

impl FakeRng {
    fn new(seed: u64) -> Self {
        FakeRng { current: seed }
    }
}

impl Default for FakeRng {
    fn default() -> Self {
        FakeRng::new(19270412)
    }
}

impl RngCore for FakeRng {
    fn next_u32(&mut self) -> u32 {
        self.current = (self.current * 1919810 + 114514) % 19260817;
        self.current as u32
    }

    fn next_u64(&mut self) -> u64 {
        self.next_u32() as u64
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        unimplemented!()
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        unimplemented!()
    }
}

lazy_static! {
    static ref A2Z_VECTOR: Vec<String> = {
        let mut ret = Vec::new();
        for c1 in b'a'..=b'z' {
            for c2 in b'a'..=b'z' {
                for c3 in b'a'..=b'z' {
                    ret.push(format!("{}{}{}", char::from(c1), char::from(c2), char::from(c3)));
                }
            }
        }
        ret
    };

    static ref CANDIDATE_VALUES: Vec<String> = {
        vec![
            "不传谣", "不信谣", "可防", "可控", "可治",
            "逮捕", "八人", "依法处理", "能", "明白",
            "鄂A0260W", "名誉会长", "帐篷", "莆田系", "心力交瘁",
            "武汉商学院", "武汉加油", "湖北加油", "中国加油", "历史会记住你们"
        ].into_iter().map(|s| s.to_string()).collect()
    };
}

pub(crate) fn gen_keys_range(start: &str, count: usize) -> Vec<String> {
    let search_vec: &Vec<String> = A2Z_VECTOR.deref();
    let start_idx = search_vec.binary_search(&start.to_string()).unwrap();
    let end_idx = start_idx + count;

    assert!(end_idx < search_vec.len());
    (&search_vec[start_idx..end_idx]).to_owned()
}

pub(crate) fn gen_values(count: usize) -> Vec<String> {
    let mut ret = Vec::new();
    let search_vec: &Vec<String> = CANDIDATE_VALUES.deref();
    for _ in 0..count {
        ret.push(search_vec[rand::thread_rng().gen_range(0, search_vec.len())].to_owned());
    }
    ret
}

pub(crate) fn gen_kv(key_start: &str, count: usize) -> Vec<KVPair> {
    let keys = gen_keys_range(key_start, count);
    let values = gen_values(count);

    keys.into_iter().zip(values.into_iter()).map(|(k, v)| KVPair(k, v)).collect()
}
