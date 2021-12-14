//! Concept stolen from the walkdir crate

use std::{io, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DevId(u64);

impl DevId {
    #[cfg(unix)]
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        path.as_ref().metadata().map(|md| Self(md.dev()))
    }

    #[cfg(windows)]
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        use winapi_util::{file, Handle};

        let h = Handle::from_path_any(path)?;
        file::information(h).map(|info| Self(info.volume_serial_number()))
    }

    #[cfg(not(any(unix, windows)))]
    pub fn new<P: AsRef<Path>>(_: P) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "DevId not supported on this platform",
        ))
    }
}
