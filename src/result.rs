/// Standard Result used in the library
pub type Result<T> = std::result::Result<T, Error>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum Error {
    /// The block was not found in the archive.
    BlockNotFound,
    /// The block already exists in the archive. This error may be returned by [BlockArchive::store_block].
    BlockExists,
    /// Not enough data read.
    NotEnoughData,
    IoError(std::io::Error),
    BitcoinSVError(bitcoinsv::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::BlockNotFound => write!(f, "Block not found"),
            Error::BlockExists => write!(f, "Block exists"),
            Error::NotEnoughData => write!(f, "Not enough data"),
            Error::IoError(err) => write!(f, "IO error: {err}"),
            Error::BitcoinSVError(err) => write!(f, "Bitcoin SV error: {err}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<bitcoinsv::Error> for Error {
    fn from(err: bitcoinsv::Error) -> Error {
        Error::BitcoinSVError(err)
    }
}
