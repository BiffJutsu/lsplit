extern crate clap;
use clap::{App, Arg, ArgMatches};
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

// TODO(cspital) add byte size constraint argument

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
            .help("Specify the maximum size of a chunk in bytes, [k|m] may be appended to the end of this number to indicate [k]ilobytes or [m]megabytes.")
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
}

#[derive(Debug)]
struct Config {
    size: u32,
    pwd: PathBuf,
    target: PathBuf,
    base: Option<PathBuf>,
}

impl Config {
    fn new(matches: &ArgMatches) -> Result<Config, String> {
        let presize = matches.value_of("bytes").unwrap();
        let size = Config::parse_size(presize)?;
        let pwd = env::current_dir().unwrap();
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

    fn parse_size(arg: &str) -> Result<u32, String> {
        match ByteSize::from_str(arg) {
            Ok(b) => Ok(b.value),
            Err(e) => Err(e),
        }
    }
}
#[derive(Debug)]
struct ByteSize {
    value: u32,
}

impl FromStr for ByteSize {
    type Err = String;
    fn from_str(arg: &str) -> Result<Self, Self::Err> {
        match arg.parse::<u32>() {
            Ok(s) => Ok(ByteSize { value: s }),
            _ => {
                let pivot = &arg.len() - 1;
                let prefix = &arg[..pivot];
                match prefix.parse::<u32>() {
                    Ok(s) => {
                        let last = &arg[pivot..];
                        match last {
                            "k" => Ok(ByteSize { value: s * 1_000 }),
                            "m" => Ok(ByteSize {
                                value: s * 1_000_000,
                            }),
                            _ => Err(format!("{} is not a support size suffix", last)),
                        }
                    }
                    _ => Err(format!(
                        "{} is not numeric, only k or m is a support size suffix",
                        prefix
                    )),
                }
            }
        }
    }
}
