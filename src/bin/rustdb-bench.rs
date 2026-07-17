#![forbid(unsafe_code)]

mod rustdb_bench;

use clap::Parser;
use rustdb_bench::Args;

fn main() {
    let args = Args::parse();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to create runtime: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.block_on(rustdb_bench::run(args)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
