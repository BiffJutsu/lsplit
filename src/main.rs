extern crate clap;
use clap::{App, Arg, ArgMatches};
use std::env;
use std::error;
use std::error::Error;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver, RecvError, SendError, Sender};
use std::thread;

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
            Arg::with_name("dir")
                .help("Optionally specify the directory into which the files will be added.")
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

    let splitter = Splitter::new(config);
    match splitter.split() {
        Ok(()) => return,
        Err(e) => println!("{}", e.description()),
    }
}

#[derive(Debug)]
struct Config {
    size: u32,
    pwd: PathBuf,
    target: PathBuf,
    dir: Option<PathBuf>,
}

impl Config {
    fn new(matches: &ArgMatches) -> ConfigResult<Config> {
        let presize = matches.value_of("bytes").unwrap();
        let size = Config::parse_size(presize)?;
        let pwd = env::current_dir()?;
        let target = PathBuf::from(matches.value_of("file").unwrap());
        if !target.is_file() {
            return Err(ConfigError::StateError("target must be a file".to_owned()));
        }

        Ok(Config {
            size,
            pwd,
            target,
            dir: match matches.value_of("dir") {
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
    StateError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigError::ByteSizeError(msg) => write!(f, "{}", msg),
            ConfigError::DirError(err) => err.fmt(f),
            ConfigError::StateError(msg) => write!(f, "{}", msg),
        }
    }
}

impl error::Error for ConfigError {
    fn description(&self) -> &str {
        match self {
            ConfigError::ByteSizeError(msg) => msg,
            ConfigError::DirError(err) => err.description(),
            ConfigError::StateError(msg) => msg,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            ConfigError::ByteSizeError(_) => None,
            ConfigError::DirError(err) => Some(err),
            ConfigError::StateError(_) => None,
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> Self {
        ConfigError::DirError(err)
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
                let pivot = arg.len() - 1;
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

type SplitterResult = Result<(), SplitterError>;
type SplitterHandle = thread::JoinHandle<SplitterResult>;

#[derive(Debug)]
enum SplitterError {
    IOError(io::Error),
    SendError(SendError<Line>),
    RecvError(RecvError),
    Temp(String),
}

impl fmt::Display for SplitterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SplitterError::IOError(e) => e.fmt(f),
            SplitterError::Temp(s) => write!(f, "{}", s),
            SplitterError::SendError(e) => e.fmt(f),
            SplitterError::RecvError(e) => e.fmt(f),
        }
    }
}

impl error::Error for SplitterError {
    fn description(&self) -> &str {
        match self {
            SplitterError::IOError(e) => e.description(),
            SplitterError::Temp(s) => s,
            SplitterError::SendError(e) => e.description(),
            SplitterError::RecvError(e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            SplitterError::IOError(e) => Some(e),
            SplitterError::Temp(_) => None,
            SplitterError::SendError(e) => Some(e),
            SplitterError::RecvError(e) => Some(e),
        }
    }
}

impl From<io::Error> for SplitterError {
    fn from(err: io::Error) -> Self {
        SplitterError::IOError(err)
    }
}

impl From<SendError<Line>> for SplitterError {
    fn from(err: SendError<Line>) -> Self {
        SplitterError::SendError(err)
    }
}

impl From<RecvError> for SplitterError {
    fn from(err: RecvError) -> Self {
        SplitterError::RecvError(err)
    }
}

struct Line {
    content: String,
    size: u32,
}

impl Line {
    fn new(content: String, size: usize) -> Self {
        Line {
            content: content,
            size: size as u32,
        }
    }
}

impl<'a> From<&'a Line> for &'a [u8] {
    fn from(line: &'a Line) -> &'a [u8] {
        line.content.as_bytes()
    }
}

impl AsRef<Line> for Line {
    fn as_ref(&self) -> &Line {
        &self
    }
}

struct Splitter {
    chunk_size: u32,
    read: PathBuf,
    write_dir: PathBuf,
}

impl Splitter {
    fn new(cfg: Config) -> Self {
        Splitter {
            chunk_size: cfg.size,
            read: cfg.target,
            write_dir: match cfg.dir {
                Some(d) => d,
                None => cfg.pwd,
            },
        }
    }

    fn split(&self) -> Result<(), SplitterError> {
        let (sender, receiver) = channel::<Line>();
        let target = fs::File::open(&self.read)?;
        let split_reader = SplitReader::new(target);
        let split_writer = SplitWriter::new(self);

        let _read_result: SplitterHandle = thread::spawn(move || Ok(split_reader.stream(sender)?));

        Ok(split_writer.stream(receiver)?)
    }
}

struct SplitWriter<'s> {
    splitter: &'s Splitter,
}

impl<'s> SplitWriter<'s> {
    fn new(splitter: &'s Splitter) -> Self {
        SplitWriter { splitter }
    }

    fn stream(&self, receiver: Receiver<Line>) -> SplitterResult {
        if let Ok(mut line) = receiver.recv() {
            let mut progress = 0;
            let mut file_num = 1;
            fs::create_dir_all(&self.splitter.write_dir)?;
            let mut writer = new_writer(file_num, self.splitter)?;
            while line.size > 0 {
                progress += line.size;
                if progress > self.splitter.chunk_size {
                    if line.size > self.splitter.chunk_size {
                        return Err(SplitterError::Temp(
                            "line size exceeds maximum allowed chunk size".to_owned(),
                        ));
                    }
                    file_num += 1;
                    progress = line.size;
                    writer.flush()?;
                    writer = new_writer(file_num, self.splitter)?;
                }
                writer.write_all(line.as_ref().into())?;
                line = receiver.recv()?;
            }
        }
        Ok(())
    }
}

fn new_writer(file_num: i32, splitter: &Splitter) -> Result<BufWriter<File>, SplitterError> {
    if let Some(new_path) = derive_new_path(file_num, splitter) {
        let new_file = File::create(new_path)?;
        return Ok(BufWriter::new(new_file));
    }
    Err(SplitterError::Temp("Invalid filename.".to_string()))
}

fn derive_new_path(file_num: i32, splitter: &Splitter) -> Option<PathBuf> {
    match splitter.read.file_name() {
        None => None,
        Some(oss) => match oss.to_str() {
            None => None,
            Some(s) => {
                let dir = PathBuf::from(&splitter.write_dir);
                Some(dir.join(format!("{}_{}", file_num, s)))
            }
        },
    }
}

#[derive(Debug)]
struct SplitReader {
    read: File,
}

impl SplitReader {
    fn new(read: File) -> Self {
        SplitReader { read }
    }

    fn stream(&self, send: Sender<Line>) -> SplitterResult {
        let mut reader = BufReader::new(&self.read);
        let mut first = String::new();
        if let Ok(mut count) = reader.read_line(&mut first) {
            send.send(Line::new(first, count))?;
            while count > 0 {
                let mut subs = String::new();
                count = reader.read_line(&mut subs)?;
                send.send(Line::new(subs, count))?;
            }
        }
        Ok(send.send(Line::new(String::new(), 0))?)
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
