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
            option_env!("OREZ_SYNC_NATIVE_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")),
            sync_core::schema_revision()
        ),
        Command::Serve(config) => {
            if std::env::var_os("OREZ_SYNC_NATIVE_PARENT_PIPE").is_some() {
                // the launcher owns stdin's write end. abrupt launcher death
                // closes it, so no signal handler is needed to stop this host.
                std::thread::spawn(|| {
                    use std::io::Read;
                    let mut buffer = [0u8; 64];
                    loop {
                        match std::io::stdin().read(&mut buffer) {
                            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                            Ok(0) | Err(_) => std::process::exit(0),
                            Ok(_) => {}
                        }
                    }
                });
            }
            if let Err(error) = serve(*config).await {
                eprintln!("error: {error}");
                std::process::exit(1);
            }
        }
    }
}
