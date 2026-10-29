use mr_rust_compat::{mrapps, KeyValue};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::process;

fn main() {
    if let Err(err) = run() {
        eprintln!("mrsequential: {err}");
        process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!("Usage: mrsequential APP INPUT...");
        process::exit(2);
    }
    let app = &args[0];
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for filename in &args[1..] {
        let contents = fs::read_to_string(filename)?;
        for KeyValue { key, value } in mrapps::map(app, filename, &contents)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?
        {
            grouped.entry(key).or_default().push(value);
        }
    }
    let output = fs::File::create("mr-out-0")?;
    let mut output = BufWriter::new(output);
    for (key, values) in grouped {
        let value = mrapps::reduce(app, &key, &values)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        writeln!(output, "{key} {value}")?;
    }
    output.flush()
}
