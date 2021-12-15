use std::{fs::File, io, io::BufReader, path::PathBuf};

use anyhow::Context;
use sha2::{Digest, Sha512};

use crate::{hash::HashMap, Meta, Result, Worker};

pub type Hash = [u8; 64];

pub fn hash(path: PathBuf, meta: Meta, worker: impl AsRef<Worker>) -> Result {
    let Worker {
        block_size,
        ref hash_for_path,
        ref file_hashes,
        ..
    } = *worker.as_ref();

    let mut file = BufReader::with_capacity(
        block_size,
        File::open(&path).with_context(|| format!("Failed to open file {:?}", path))?,
    );

    let mut hasher = Sha512::new();
    io::copy(&mut file, &mut hasher).with_context(|| format!("Failed to hash {:?}", path))?;
    let hash = hasher.finalize();

    let mut bytes = [0u8; 64];
    bytes[..].copy_from_slice(hash.as_slice());

    if hash_for_path.insert(path.clone(), bytes).is_none() {
        assert!(
            file_hashes
                .entry(bytes)
                .or_insert_with(HashMap::default)
                .insert(path, meta)
                .is_none()
        );
    }

    Ok(())
}
