extern crate clap;
use clap::{App, Arg};

// TODO(cspital) add byte size constraint argument

fn main() {
    let _matches = App::new("By Line File Splitter")
        .version("0.1.0")
        .author("Cliff Spital <cspital@uw.edu>")
        .about("Splits a file on line ending, to chunks of specified size.")
        .arg(
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

    println!("Hello, world!");
}
