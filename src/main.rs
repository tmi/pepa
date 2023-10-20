use std::path::Path;
use clap::Parser;
use inpa::summarize_parquet_metadata;

#[derive(Parser, Debug)]
struct Args {
    file_path: String,

    #[arg(short, long, default_value_t = 1)]
    level: u8,
}

fn main() {
    let args = Args::parse();

    let file_path = Path::new(&args.file_path);
    let summary = summarize_parquet_metadata(file_path, args.level);

    println!("{summary}")
}
