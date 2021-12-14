use std::{
    collections::{HashMap, HashSet},
    fs,
    fs::File,
    io,
    io::{prelude::*, BufReader},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Error};
use crossbeam::{channel, queue::SegQueue};
use crossterm::{cursor, queue, terminal};
use dashmap::DashMap;
use rayon::{iter, prelude::*, ThreadPoolBuilder};
use sha2::{Digest, Sha256};
use structopt::StructOpt;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Default concurrency limit
const DEFAULT_JOBS: usize = 4;

#[derive(StructOpt)]
struct Opts {
    #[structopt(parse(from_os_str))]
    dir: PathBuf,

    #[structopt(parse(from_os_str), short, long, default_value = "latke.bin")]
    cache: PathBuf,

    #[structopt(parse(from_os_str), short, long, default_value = "hashes.json")]
    output: PathBuf,

    /// Concurrency limit.  Default is 4.  If the flag but no value is
    /// specified, defaults to the number of available cores.
    #[structopt(short, long)]
    jobs: Option<Option<usize>>,
}

fn main() {
    fn flush(
        dir: impl AsRef<Path>,
        map: &HashMap<PathBuf, Vec<u8>>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let tmp = PathBuf::from(format!("{}~", path.as_ref().to_string_lossy()));
        let file = File::create(&tmp).context("failed to open temporary file")?;

        bincode::serialize_into(file, &(dir.as_ref(), map)).context("failed to serialize data")?;

        fs::rename(tmp, path).context("failed to overwrite cache file")?;

        Ok(())
    }

    fn load(path: impl AsRef<Path>) -> Result<(PathBuf, HashMap<PathBuf, Vec<u8>>)> {
        let file = File::open(path)?;

        Ok(bincode::deserialize_from(file)?)
    }

    fn print_err(
        stderr: &Mutex<io::Stderr>,
        e: impl Into<Error>,
        cb: impl FnOnce(&mut io::Stderr) -> io::Result<()>,
    ) {
        let stderr = &mut *stderr.lock().unwrap();
        queue!(
            stderr,
            cursor::RestorePosition,
            terminal::Clear(terminal::ClearType::CurrentLine),
        )
        .unwrap();

        cb(stderr).unwrap();
        writeln!(stderr, ": {:?}", e.into()).unwrap();

        queue!(stderr, cursor::SavePosition).unwrap();
        stderr.flush().unwrap();
    }

    let Opts {
        dir,
        cache,
        output,
        jobs,
    } = Opts::from_args();
    let (tx, rx) = channel::unbounded();

    let (loaded, loaded_names) = if let Ok((loaded_path, loaded)) = load(&cache).map_err(|e| {
        eprintln!("Didn't load cache: {:?}", e);
    }) {
        let loaded = if loaded_path == dir {
            loaded
        } else {
            eprintln!("Cache is for different path {:?}; ignoring.", loaded_path);
            HashMap::new()
        };

        let loaded_names: HashSet<_> = loaded.keys().cloned().collect();

        eprintln!("Loaded {} filename(s)", loaded_names.len());

        (loaded, loaded_names)
    } else {
        (HashMap::new(), HashSet::new())
    };

    let mut stderr = io::stderr();
    queue!(stderr, cursor::SavePosition).unwrap();
    stderr.flush().unwrap();

    let stderr = Mutex::new(stderr);

    let mut b = ThreadPoolBuilder::new();


    if let Some(j) = jobs.unwrap_or(Some(DEFAULT_JOBS + 1)) {
        b = b.num_threads(j);
    }

    let (map, ()) = b
        .build()
        .expect("failed to initialize thread pool")
        .install(|| {
            rayon::join(
                || {
                    let mut map = loaded;
                    let mut last_read = Instant::now();

                    while let Some((path, hash)) =
                        rx.recv().expect("failed to receive completed hash")
                    {
                        map.insert(path, hash);

                        let now = Instant::now();

                        if now - last_read > Duration::from_secs_f64(5.0) {
                            last_read = now;
                            match flush(&dir, &map, &cache) {
                                Ok(()) => (),
                                Err(e) => {
                                    print_err(&stderr, e, |s| write!(s, "Failed to save cache"));
                                },
                            };
                        }
                    }

                    map
                },
                || {
                    let q = SegQueue::new();
                    let done = AtomicUsize::new(0);

                    q.push(dir.clone());

                    while !q.is_empty() {
                        iter::repeat(())
                            .map(|()| {
                                q.pop().map(|path| {
                                    let meta = match fs::metadata(&path) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            print_err(&stderr, e, |s| {
                                                write!(s, "Skipping path {:?}", path)
                                            });

                                            return;
                                        },
                                    };

                                    let curr_done = done.fetch_add(1, Ordering::Relaxed) + 1;

                                    {
                                        let stderr = &mut *stderr.lock().unwrap();
                                        queue!(
                                            stderr,
                                            cursor::RestorePosition,
                                            cursor::SavePosition,
                                            terminal::Clear(terminal::ClearType::CurrentLine)
                                        )
                                        .unwrap();
                                        write!(
                                            stderr,
                                            "({}/{}?) {:?}",
                                            curr_done,
                                            curr_done + q.len(),
                                            path
                                        )
                                        .unwrap();
                                        stderr.flush().unwrap();
                                    }

                                    if meta.is_dir() {
                                        match fs::read_dir(&path) {
                                            Ok(r) => {
                                                for entry in r {
                                                    q.push(
                                                        entry
                                                            .expect("failed to enumerate directory")
                                                            .path(),
                                                    );
                                                }
                                            },
                                            Err(e) => print_err(&stderr, e, |s| {
                                                write!(s, "Skipping directory {:?}", path)
                                            }),
                                        }
                                    } else if meta.is_file() && !loaded_names.contains(&path) {
                                        match path.to_str() {
                                            Some(_) => (),
                                            None => {
                                                print_err(
                                                    &stderr,
                                                    anyhow::anyhow!(
                                                        "File name can't be serialized"
                                                    ),
                                                    |s| write!(s, "Skipping file {:?}", path),
                                                );
                                                return;
                                            },
                                        }

                                        let mut fs = match File::open(&path) {
                                            Ok(f) => BufReader::with_capacity(4 * 1024 * 1024, f),
                                            Err(e) => {
                                                print_err(&stderr, e, |s| {
                                                    write!(s, "Skipping file {:?}", path)
                                                });
                                                return;
                                            },
                                        };

                                        let mut hasher = Sha256::new();
                                        match io::copy(&mut fs, &mut hasher) {
                                            Ok(_n) => (),
                                            Err(e) => {
                                                print_err(&stderr, e, |s| {
                                                    write!(s, "Skipping file {:?}", path)
                                                });
                                                return;
                                            },
                                        }
                                        let hash = hasher.finalize();

                                        let mut bytes = vec![0u8; 32];
                                        bytes[..].copy_from_slice(hash.as_slice());

                                        tx.send(Some((path, bytes)))
                                            .expect("failed to send completed hash");
                                    }
                                })
                            })
                            .while_some()
                            .for_each(|()| ());
                    }

                    tx.send(None).expect("failed to send sentinel");
                },
            )
        });

    let mut stderr = stderr.into_inner().unwrap();

    queue!(
        stderr,
        cursor::RestorePosition,
        cursor::SavePosition,
        terminal::Clear(terminal::ClearType::CurrentLine)
    )
    .unwrap();
    writeln!(stderr, "{} file(s) processed", map.len()).unwrap();
    stderr.flush().unwrap();

    match flush(&dir, &map, &cache) {
        Ok(()) => (),
        Err(e) => writeln!(stderr, "Failed to save final cache: {:?}", e).unwrap(),
    }

    writeln!(stderr, "Reversing...").unwrap();

    let rev = DashMap::new();
    map.par_iter().for_each(|(k, v)| {
        rev.entry(v).or_insert_with(Vec::new).push(k);
    });

    let rev: DashMap<_, _> = rev
        .into_par_iter()
        .map(|(k, v)| {
            (
                k.iter()
                    .map(|b| format!("{:02x}", b))
                    .fold(String::new(), |mut s, h| {
                        s.push_str(&h);
                        s
                    }),
                v,
            )
        })
        .collect();

    let file = File::create(output).expect("failed to open output file");
    serde_json::to_writer(file, &rev).expect("failed to write output JSON");
}
