use sync_native::standalone::{Command, parse_args, serve};

#[tokio::main]
async fn main() {
    let command = parse_args(std::env::args().skip(1)).unwrap_or_else(|error| {
        eprintln!("error: {error}\n\n{}", sync_native::standalone::USAGE);
        std::process::exit(2);
    });
    match command {
        Command::Help => println!("{}", sync_native::standalone::USAGE),
        // the schema revision names the durable contract this binary can read
        // (packed-ledger format included). release tooling compares it against
        // the source tree because the package version alone has shipped stale:
        // npm 0.1.2 and a packed-ledger build both called themselves 0.1.2
        // while only one of them could see acks.
        Command::Version => println!(
            "sync-native {} {}",
            env!("CARGO_PKG_VERSION"),
            sync_core::schema_revision()
        ),
        Command::Serve(config) => {
            if let Err(error) = serve(*config).await {
                eprintln!("error: {error}");
                std::process::exit(1);
            }
        }
    }
}
