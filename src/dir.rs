use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context};
use log::{error, info, warn};
use topograph::{graph::DependencyBag, prelude::*};

use crate::{
    dev_id::DevId,
    hash::{HashMap, HashSet},
    Item, Job, Result, Worker,
};

pub fn recurse(
    path: PathBuf,
    root_id: Option<DevId>,
    handle: crate::Handle,
    worker: impl AsRef<Worker>,
) -> Result {
    let Worker { ref seen, .. } = *worker.as_ref();

    let mut children = Vec::new();
    let mut child_paths = HashSet::default();

    for child in
        fs::read_dir(&path).with_context(|| format!("Failed to open directory {:?}", path))?
    {
        match child.and_then(|c| Ok((c.path(), c.metadata()?))) {
            Ok((path, meta)) => {
                child_paths.insert(Item::new(path.clone(), meta.clone()));

                if seen.contains(&path) {
                    continue;
                }

                if let Some(job) = Job::path(path, meta, root_id, worker.as_ref())? {
                    children.push(job);
                }
            },
            Err(e) => error!("Error while reading directory {:?}: {:?}", path, e),
        }
    }

    let mut deps = handle.create_node_or_run(Job::FinalizeDir(path, child_paths), children.len());

    for job in children {
        handle.push_dependency(job, deps.as_mut().map(DependencyBag::take));
    }

    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
pub fn finalize(
    path: impl AsRef<Path>,
    children: HashSet<Item>,
    worker: impl AsRef<Worker>,
) -> Result {
    let Worker {
        ref hash_for_path,
        ref file_hashes,
        ..
    } = *worker.as_ref();
    let path = path.as_ref();

    match fs::read_dir(&path)
        .with_context(|| format!("Failed to open directory {:?}", path))
        .and_then(|c| {
            c.map(|c| {
                c.and_then(|c| c.metadata().map(|m| Item::new(c.path(), m)))
                    .with_context(|| format!("Error while reading directory {:?}", path))
            })
            .collect()
        })
        .and_then(|c: HashSet<_>| {
            if c == children {
                Ok(())
            } else {
                Err(anyhow!("File list changed!"))
            }
        }) {
        Ok(()) => (),
        Err(e) => warn!(
            "Failed to verify file list for directory {:?}: {:?}",
            path, e
        ),
    }

    let child_count = children.len();
    let mut child_infos = HashMap::default();

    for child in children {
        let info = match &child {
            Item::File(path, _) => {
                let hash = hash_for_path.get(path).unwrap().value();

                let mut info = file_hashes.get(hash).unwrap().value().clone();
                assert!(info.remove(path).is_some());

                info
            },
            Item::Dir(_path, _) => todo!("Handle dir"),
            Item::Symlink(_path, _) => todo!("Handle symlink"),
        };

        assert!(child_infos.insert(child, info).is_none());
    }

    info!(
        "{:?}: {}/{} - {:?}",
        path,
        child_infos.len(),
        child_count,
        child_infos
    );

    Ok(())
}
