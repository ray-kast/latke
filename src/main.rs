#![warn(clippy::pedantic, clippy::cargo)]

mod dev_id;
mod hash;

use std::{
    ffi::OsStr,
    fs,
    fs::{File, Metadata},
    io,
    io::BufReader,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Context;
use clap::Parser;
use dev_id::DevId;
use hash::{DashMap, DashSet};
use log::{error, info, trace};
use sha2::{Digest, Sha512};
use topograph::{graph, graph::DependencyBag, prelude::*, threaded};

pub type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Job {
    File(PathBuf),
    Dir(PathBuf, Option<DevId>),
    Symlink(PathBuf),
    FinalizeDir(PathBuf),
}

impl Job {
    fn path(
        path: PathBuf,
        meta: &Metadata,
        root_id: Option<DevId>,
        worker: &Worker,
    ) -> Result<Option<Self>> {
        let path_id =
            DevId::new(&path).with_context(|| format!("Failed to get device ID for {:?}", path))?;

        if root_id.map_or(false, |r| r == path_id) {
            return Ok(None);
        }

        Ok(Some(if meta.is_symlink() {
            worker.total_files.fetch_add(1, Ordering::Relaxed);
            Job::Symlink(path)
        } else if meta.is_dir() {
            worker.total_dirs.fetch_add(1, Ordering::Relaxed);
            Job::Dir(path, root_id)
        } else if meta.is_file() {
            worker.total_files.fetch_add(1, Ordering::Relaxed);
            Job::File(path)
        } else {
            unreachable!();
        }))
    }
}

#[derive(Debug)]
pub struct Worker {
    block_size: usize,
    files_done: AtomicUsize,
    dirs_done: AtomicUsize,
    total_files: AtomicUsize,
    total_dirs: AtomicUsize,
    seen: AssertUnwindSafe<DashSet<PathBuf>>,
    hashes: AssertUnwindSafe<DashMap<PathBuf, [u8; 64]>>,
}

impl Worker {
    fn tally(&self, job: &Job) -> bool {
        let path = match job {
            Job::File(p) | Job::Symlink(p) => {
                self.files_done.fetch_add(1, Ordering::Relaxed);
                p
            },
            Job::Dir(p, _) => {
                self.dirs_done.fetch_add(1, Ordering::Relaxed);
                p
            },
            Job::FinalizeDir(_) => return true,
        };

        self.seen.insert(path.clone())
    }
}

/// Compute the hashes of files to locate possible duplicate files and
/// directories.
#[derive(Debug, Parser)]
#[clap(version, author)]
struct Opts {
    /// Base directories to search
    #[clap(parse(try_from_os_str = parse_path), required = true)]
    paths: Vec<(PathBuf, Metadata)>,

    /// Maximum number of threads to use.  Set to 0 to use all available cores.
    #[clap(short = 'j', default_value_t = 4)]
    threads: usize,

    /// Block size to read files in
    #[clap(short, long, default_value_t = 4 * 1024 * 1024)]
    block_size: usize,

    /// Allow the directory search to cross filesystem boundaries.  This is
    /// likely not desirable in most cases.
    #[clap(short = 'x', long)]
    cross_filesystems: bool,
}

fn parse_path(path: &OsStr) -> Result<(PathBuf, Metadata)> {
    let path = PathBuf::from(path);
    let meta = fs::metadata(&path)?;

    Ok((path, meta))
}

fn main() {
    env_logger::init();
    let opts = Opts::parse();

    match run(opts) {
        Ok(()) => (),
        Err(e) => {
            error!("Program exited with error: {:?}", e);
            std::process::exit(-1);
        },
    }
}

fn run(
    Opts {
        paths,
        threads,
        block_size,
        cross_filesystems,
    }: Opts,
) -> Result {
    let threads = if threads == 0 { None } else { Some(threads) };

    let worker = Arc::new(Worker {
        block_size,
        files_done: AtomicUsize::new(0),
        dirs_done: AtomicUsize::new(0),
        total_files: AtomicUsize::new(0),
        total_dirs: AtomicUsize::new(0),
        seen: AssertUnwindSafe(DashSet::default()),
        hashes: AssertUnwindSafe(DashMap::default()),
    });
    let worker2 = worker.clone();

    let pool = threaded::Builder::default()
        .num_threads(threads)
        .build_graph(move |j, h| process(j, h, &worker2).map_err(|e| error!("Job failed: {:?}", e)))
        .context("Failed to initialize thread pool")?;

    for (path, meta) in paths {
        let root_id = if cross_filesystems {
            None
        } else {
            Some(
                DevId::new(&path)
                    .with_context(|| format!("Failed to get root device ID for path {:?}", path))?,
            )
        };

        if let Some(job) = Job::path(path, &meta, root_id, &worker)? {
            pool.push(job);
        }
    }

    pool.join();

    Ok(())
}

fn process(
    job: Job,
    handle: graph::Handle<threaded::Handle<graph::Job<Job>>>,
    worker: &Arc<Worker>,
) -> Result {
    trace!("{:?}", job);

    let Worker {
        block_size,
        // ref files_done,
        // ref dirs_done,
        // ref total_files,
        // ref total_dirs,
        ref seen,
        ref hashes,
        .. // TODO: remove
    } = **worker;

    if !worker.tally(&job) {
        return Ok(()); // Nothing to do
    }

    match job {
        Job::File(path) => {
            let mut file = BufReader::with_capacity(
                block_size,
                File::open(&path).with_context(|| format!("Failed to open file {:?}", path))?,
            );

            let mut hasher = Sha512::new();
            io::copy(&mut file, &mut hasher)
                .with_context(|| format!("Failed to hash {:?}", path))?;
            let hash = hasher.finalize();

            let mut bytes = [0u8; 64];
            bytes[..].copy_from_slice(hash.as_slice());

            assert!(hashes.insert(path, bytes).is_none());
        },
        Job::Dir(path, root_id) => {
            let mut children = Vec::new();

            for child in fs::read_dir(&path)
                .with_context(|| format!("Failed to open directory {:?}", path))?
            {
                match child.and_then(|c| Ok((c.path(), c.metadata()?))) {
                    Ok((path, meta)) => {
                        info!("{:?}", path);

                        if seen.contains(&path) {
                            continue;
                        }

                        if let Some(job) = Job::path(path, &meta, root_id, worker)? {
                            children.push(job);
                        }
                    },
                    Err(e) => error!("Error while reading directory {:?}: {:?}", path, e),
                }
            }

            let mut deps = handle.create_node_or_run(Job::FinalizeDir(path), children.len());

            for job in children {
                handle.push_dependency(job, deps.as_mut().map(DependencyBag::take));
            }
        },
        Job::Symlink(path) => todo!("Handle symlink {:?}", path),
        Job::FinalizeDir(path) => {
            todo!("Handle finalizing dir {:?}", path)
        },
    }

    Ok(())
}
