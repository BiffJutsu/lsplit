extern crate clap;
use clap::{App, Arg, ArgMatches};
use std::env;
use std::error;
use std::fmt;
use std::fs;
use std::io;
use std::path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc;

// TODO(cspital) components needed for performance, reader thread should stream lines to writer thread

fn main() {
    let matches = App::new("By Line File Splitter")
        .version("0.1.0")
        .author("Cliff Spital <cspital@uw.edu>")
        .about("Splits a file on line ending, to chunks of specified size.")
        .arg(
            Arg::with_name("bytes")
            .value_name("bytes")
            .short("b")
            .long("bytes")
            .help("Specify the maximum size of a chunk in bytes, [k|m] may be appended to the end of this number to indicate [k]ilobytes or [m]egabytes.")
            .required(true)
        ).arg(
            Arg::with_name("file")
                .help("Specifies the file to split.")
                .required(true)
                .index(1),
        ).arg(
            Arg::with_name("base")
                .help("Optionally specify the base filename to which the prefix will be added.")
                .required(false)
                .index(2),
        ).get_matches();

    let config = match Config::new(&matches) {
        Ok(c) => c,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    println!("{:?}", config);
}

#[derive(Debug)]
struct Config {
    size: u32,
    pwd: PathBuf,
    target: PathBuf,
    base: Option<PathBuf>,
}

impl Config {
    // TODO(cspital) fix this with custom error type that From's the errors in this function
    fn new(matches: &ArgMatches) -> ConfigResult<Config> {
        let presize = matches.value_of("bytes").unwrap();
        let size = Config::parse_size(presize)?;
        let pwd = match env::current_dir() {
            Ok(buf) => buf,
            Err(e) => return Err(ConfigError::DirError(e)),
        };
        let target = PathBuf::from(matches.value_of("file").unwrap());

        Ok(Config {
            size: size,
            pwd: pwd,
            target: target,
            base: match matches.value_of("base") {
                Some(s) => Some(PathBuf::from(s)),
                None => None,
            },
        })
    }

    #[inline]
    fn parse_size(arg: &str) -> ConfigResult<u32> {
        match arg.parse::<ByteSize>() {
            Ok(b) => {
                let ByteSize(s) = b;
                Ok(s)
            }
            Err(e) => Err(e),
        }
    }
}

type ConfigResult<T> = std::result::Result<T, ConfigError>;
#[derive(Debug)]
enum ConfigError {
    ByteSizeError(String),
    DirError(io::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigError::ByteSizeError(msg) => write!(f, "{}", msg),
            ConfigError::DirError(err) => err.fmt(f),
        }
    }
}

impl error::Error for ConfigError {
    fn description(&self) -> &str {
        match self {
            ConfigError::ByteSizeError(msg) => msg,
            ConfigError::DirError(err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            ConfigError::ByteSizeError(_) => None,
            ConfigError::DirError(err) => Some(err),
        }
    }
}

#[derive(Debug)]
struct ByteSize(u32);

impl FromStr for ByteSize {
    type Err = ConfigError;
    fn from_str(arg: &str) -> Result<Self, Self::Err> {
        match arg.parse::<u32>() {
            Ok(s) => Ok(ByteSize(s)),
            _ => {
                let pivot = &arg.len() - 1;
                let prefix = &arg[..pivot];
                match prefix.parse::<u32>() {
                    Ok(s) => {
                        let last = &arg[pivot..];
                        match last {
                            "k" => Ok(ByteSize(s * 1_000)),
                            "m" => Ok(ByteSize(s * 1_000_000)),
                            _ => Err(ConfigError::ByteSizeError(format!(
                                "{} is not a support size suffix",
                                last
                            ))),
                        }
                    }
                    _ => Err(ConfigError::ByteSizeError(format!(
                        "{} is not numeric, only k or m is a supported size suffix",
                        prefix
                    ))),
                }
            }
        }
    }
}
// TODO(cspital) transform to enum to accept string and io error
#[derive(Debug)]
struct SplitterPermissionError(String);
impl fmt::Display for SplitterPermissionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let SplitterPermissionError(msg) = self;
        write!(f, "{}", msg)
    }
}

impl error::Error for SplitterPermissionError {
    fn description(&self) -> &str {
        let SplitterPermissionError(msg) = self;
        msg
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

// TODO(cspital) this is responsible to starting the reader/writer threads and running the pipeline
struct Splitter {
    chunk_size: u32,
    read: PathBuf,
    write_dir: PathBuf,
    base: Option<PathBuf>,
}

impl Splitter {
    fn new(cfg: Config) -> Result<Self, SplitterPermissionError> {
        // TODO(cspital) calculate write directory from base
        Err(SplitterPermissionError("not implemented".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bytesize_fromstr_numeric_ok() {
        let input = "2000";

        let ByteSize(size) = input.parse::<ByteSize>().unwrap();
        assert_eq!(size, 2000);
    }

    #[test]
    fn bytesize_fromstr_kilo_ok() {
        let input = "2k";

        let ByteSize(size) = input.parse::<ByteSize>().unwrap();
        assert_eq!(size, 2000);
    }

    #[test]
    fn bytesize_fromstr_mega_ok() {
        let input = "2m";

        let ByteSize(size) = input.parse::<ByteSize>().unwrap();
        assert_eq!(size, 2_000_000);
    }

    #[test]
    fn bytesize_fromstr_invalid() {
        let input = "2km";

        let size = input.parse::<ByteSize>();
        assert!(size.is_err());
    }
}
