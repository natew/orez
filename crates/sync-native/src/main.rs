use sync_native::standalone::{Command, parse_args, serve};

#[tokio::main]
async fn main() {
    let command = parse_args(std::env::args().skip(1)).unwrap_or_else(|error| {
        eprintln!("error: {error}\n\n{}", sync_native::standalone::USAGE);
        std::process::exit(2);
    });
    match command {
        Command::Help => println!("{}", sync_native::standalone::USAGE),
        Command::Version => println!("sync-native {}", env!("CARGO_PKG_VERSION")),
        Command::Serve(config) => {
            if let Err(error) = serve(*config).await {
                eprintln!("error: {error}");
                std::process::exit(1);
            }
        }
    }
}
