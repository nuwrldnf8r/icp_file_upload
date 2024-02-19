use ic_stable_structures::memory_manager::{MemoryId, MemoryManager, VirtualMemory};
use ic_stable_structures::{storable::Bound, DefaultMemoryImpl, StableBTreeMap, Storable,};
use std::{borrow::Cow, cell::RefCell};
use sha2::{Digest, Sha256};
use ic_cdk::{
    query, update
};

const MAX_VALUE_SIZE: u32 = 100000;

#[derive(Clone)]
struct File {
    pub chunks: Vec<u128>,
}


impl Storable for File {
    fn to_bytes(&self) -> std::borrow::Cow<[u8]> {
        let bytes = self.chunks.iter().flat_map(|&id| id.to_le_bytes().to_vec()).collect();
        Cow::Owned(bytes)
    }
    fn from_bytes(bytes: std::borrow::Cow<[u8]>) -> Self {
        let num_u128 = bytes.len() / std::mem::size_of::<u128>();
        let mut chunks = Vec::with_capacity(num_u128);
        for i in 0..num_u128 {
            let start = i * std::mem::size_of::<u128>();
            let end = start + std::mem::size_of::<u128>();
            let bytes = &bytes[start..end];
            let mut array = [0; std::mem::size_of::<u128>()];
            array.copy_from_slice(bytes);
            chunks.push(u128::from_le_bytes(array));
        }
        File { chunks }
    }
    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_VALUE_SIZE,
        is_fixed_size: false,
    };
}

#[derive(Clone)]
struct Chunk{
    pub data: Vec<u8>,
}

impl Storable for Chunk {
    fn to_bytes(&self) -> std::borrow::Cow<[u8]> {
        Cow::Borrowed(&self.data)
    }

    fn from_bytes(bytes: std::borrow::Cow<[u8]>) -> Self {
        let data = bytes.to_vec();
        Chunk { data }
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_VALUE_SIZE,
        is_fixed_size: false,
    };
}


//type Memory = VirtualMemory<DefaultMemoryImpl>;

thread_local! {
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
        RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));
    static FILE_MAP: RefCell<StableBTreeMap<u128, File, VirtualMemory<DefaultMemoryImpl>>> = RefCell::new(
        StableBTreeMap::init(
            MEMORY_MANAGER.with(|m| m.borrow().get(MemoryId::new(0))),
        )
    );
    static CHUNK_MAP: RefCell<StableBTreeMap<u128, Chunk, VirtualMemory<DefaultMemoryImpl>>> = RefCell::new(
        StableBTreeMap::init(
            MEMORY_MANAGER.with(|m| m.borrow().get(MemoryId::new(1))),
        )
    );
}

fn get_id(blob: &[u8]) -> u128 {
    let mut hasher = Sha256::new();
    hasher.update(blob);
    let hash_result = hasher.finalize();
    let mut result = 0u128;
    for i in 0..16 {
        result |= (hash_result[i] as u128) << (8 * i);
    }
    result
}

#[update]
fn upload(blob: Vec<u8>) -> String {
    let file_id = get_id(&blob);
    FILE_MAP.with(|file_map|{
        let mut file_map_mut = file_map.borrow_mut();
        if !file_map_mut.contains_key(&file_id){
            let chunk_size = 1024;
            let num_chunks = (blob.len() + chunk_size - 1) / chunk_size;
            let chunks: Vec<Chunk> = (0..num_chunks).map(|i| {
                let start = i * chunk_size;
                let end = std::cmp::min((i + 1) * chunk_size, blob.len());
                let data = blob[start..end].to_vec();
                Chunk { data }
            }).collect();
            let ids: Vec<u128> = (0..num_chunks).map(|i|{
                get_id(chunks[i].data.as_slice())
            }).collect();
            CHUNK_MAP.with(|chunk_map|{
                let mut chunk_map_mut = chunk_map.borrow_mut();
                for i in 0..num_chunks{
                    if !chunk_map_mut.contains_key(&ids[i]){
                        chunk_map_mut.insert(ids[i], chunks[i].clone());
                    }
                }
                file_map_mut.insert(file_id.clone(), File{ chunks: ids });
            })
        } 
    });
    format!("{:x}", file_id)
    
}

fn hex_string_to_u128(hex_string: &str) -> Result<u128, std::num::ParseIntError> {
    u128::from_str_radix(hex_string, 16)
}

#[query]
fn get(file_id: String) -> Vec<u8>{
    let file_id_u128: u128;
    match hex_string_to_u128(&file_id){
        Ok(result) => file_id_u128 = result,
        Err(_) => panic!("Invalid file_id")
    }
    FILE_MAP.with(|file_map|{
        let file_map_borrowed = file_map.borrow();
        if !file_map_borrowed.contains_key(&file_id_u128){
            Vec::new()
        } else {
            let file = file_map_borrowed.get(&file_id_u128).unwrap();
            CHUNK_MAP.with(|chunk_map|{
                let chunk_map_borrowed = chunk_map.borrow();
                file.chunks.as_slice().iter().flat_map(|chunk_id|{
                    let chunk = chunk_map_borrowed.get(&chunk_id).unwrap();
                    chunk.data
                }).collect()
                //Vec::new()
            })
        }
        
    })
}

