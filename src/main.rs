#![warn(clippy::pedantic, clippy::cargo)]

mod dev_id;
mod dir;
mod file;
mod hash;

use std::{
    cmp,
    cmp::{Eq, Ord, PartialEq, PartialOrd},
    ffi::OsStr,
    fmt,
    fmt::{Display, Formatter},
    fs,
    fs::Metadata,
    hash::{Hash, Hasher},
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context};
use clap::Parser;
use dev_id::DevId;
use hash::{DashMap, DashSet, HashMap, HashSet};
use log::{error, trace, warn};
use topograph::{graph, prelude::*, threaded};

type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

// May change later
type Meta = Metadata;

#[derive(Debug, Clone)]
pub enum Item {
    File(PathBuf, Meta),
    Dir(PathBuf, Meta),
    Symlink(PathBuf, Meta),
}

impl Display for Item {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::File(p, _) => write!(f, "File {:?}", p),
            Self::Dir(p, _) => write!(f, "Directory {:?}", p),
            Self::Symlink(p, _) => write!(f, "Symlink {:?}", p),
        }
    }
}

impl Ord for Item {
    fn cmp(&self, rhs: &Item) -> cmp::Ordering {
        let ret = self.path().cmp(rhs.path());
        debug_assert!(!matches!(ret, cmp::Ordering::Equal) || self == rhs);
        ret
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, rhs: &Item) -> Option<cmp::Ordering> { Some(self.cmp(rhs)) }
}

impl Eq for Item {}
impl PartialEq for Item {
    fn eq(&self, rhs: &Item) -> bool { self.path() == rhs.path() }
}

impl Hash for Item {
    fn hash<H: Hasher>(&self, hash: &mut H) { self.path().hash(hash) }
}

impl Item {
    fn new(path: PathBuf, meta: Metadata) -> Self {
        if meta.is_symlink() {
            Self::Symlink(path, meta)
        } else if meta.is_dir() {
            Self::Dir(path, meta)
        } else if meta.is_file() {
            Self::File(path, meta)
        } else {
            unreachable!();
        }
    }

    fn path(&self) -> &PathBuf {
        match self {
            Self::File(p, _) | Self::Dir(p, _) | Self::Symlink(p, _) => p,
        }
    }
}

#[derive(Debug)]
pub enum Job {
    Item(Item, Option<DevId>),
    FinalizeDir(PathBuf, HashSet<Item>),
}

impl Display for Job {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Item(i, _) => write!(f, "{}", i),
            Self::FinalizeDir(p, c) => write!(f, "Finalize dir ({}) {:?}", c.len(), p),
        }
    }
}

impl Job {
    fn path(
        path: PathBuf,
        meta: Metadata,
        root_id: Option<DevId>,
        worker: &Worker,
    ) -> Result<Option<Self>> {
        let path_id =
            DevId::new(&path).with_context(|| format!("Failed to get device ID for {:?}", path))?;

        if root_id.map_or(false, |r| r != path_id) {
            return Ok(None);
        }

        let item = Item::new(path, meta);

        match item {
            Item::File(..) | Item::Symlink(..) => {
                worker.total_files.fetch_add(1, Ordering::Relaxed);
            },
            Item::Dir(..) => {
                worker.total_dirs.fetch_add(1, Ordering::Relaxed);
            },
        }

        Ok(Some(Self::Item(item, root_id)))
    }
}

type Handle<'a> = graph::Handle<threaded::Handle<'a, graph::Job<Job>>>;

#[derive(Debug)]
pub struct Worker {
    block_size: usize,
    files_done: AtomicUsize,
    dirs_done: AtomicUsize,
    total_files: AtomicUsize,
    total_dirs: AtomicUsize,
    seen: AssertUnwindSafe<DashSet<PathBuf>>,
    hash_for_path: AssertUnwindSafe<DashMap<PathBuf, file::Hash>>,
    file_hashes: AssertUnwindSafe<DashMap<file::Hash, HashMap<PathBuf, Metadata>>>,
}

impl Worker {
    fn tally(&self, job: &Job) -> bool {
        let path = match job {
            Job::Item(Item::File(p, _) | Item::Symlink(p, _), _) => {
                self.files_done.fetch_add(1, Ordering::Relaxed);
                p
            },
            Job::Item(Item::Dir(p, _), _) => {
                self.dirs_done.fetch_add(1, Ordering::Relaxed);
                p
            },
            Job::FinalizeDir(..) => return true,
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
        hash_for_path: AssertUnwindSafe(DashMap::default()),
        file_hashes: AssertUnwindSafe(DashMap::default()),
    });
    let worker2 = worker.clone();

    let pool = threaded::Builder::default()
        .num_threads(threads)
        .lifo(true)
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

        if let Some(job) = Job::path(path, meta, root_id, &worker)? {
            pool.push(job);
        }
    }

    pool.join();

    Ok(())
}

fn process(job: Job, handle: Handle, worker: &Arc<Worker>) -> Result {
    trace!("{}", job);

    if !worker.tally(&job) {
        return Ok(()); // Nothing to do
    }

    match job {
        Job::Item(Item::File(path, meta), _) => file::hash(path, meta, worker),
        Job::Item(Item::Dir(path, _), root_id) => dir::recurse(path, root_id, handle, worker),
        Job::Item(Item::Symlink(path, _), _) => bail!("TODO: Handle symlink {:?}", path),
        Job::FinalizeDir(path, children) => dir::finalize(&path, children, worker),
    }
}
