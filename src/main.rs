use std::path::Path;
use clap::Parser;
use pepa::summarize_parquet_metadata;
use std::process;
use serde_json::{Value, Map};

#[derive(Parser, Debug)]
struct Args {
    file_path: String,

    #[arg(short, long, default_value_t = 1)]
    level: u8, // TODO or enum instead? For better validation, help string, exh matching, ...

    #[arg(long, default_value_t = false)]
    jsonl: bool,
}

fn main() {
    let args = Args::parse();

    let file_path = Path::new(&args.file_path);
    if !file_path.exists() {
        eprintln!("File path {} does not exist.", args.file_path);
        process::exit(1);
    }
    if args.level > 2 {
        eprintln!("Level {} unsupported.", args.level);
        process::exit(1);
    }
    // TODO turn validation into collect-then-crash?

    let mut result: Map<String, Value> = Map::new();
    summarize_parquet_metadata(file_path, args.level, &mut result);

    let summary = match args.jsonl {
        false => serde_json::to_string_pretty(&result).unwrap(),
        true => serde_json::to_string(&result).unwrap(),
    };
    println!("{summary}")
}
