use anyhow::Error;

mod cli;

fn main() {
    match run_cli() {
        Ok(_) => {},
        Err(err) => eprintln!("Error: {}", err),
    }
}

fn run_cli() -> Result<(), Error> {
    use clap::{Arg, App};
    use oxigraph::SledStore;

    let matches = cli::build_cli().get_matches();

    let db_path = matches.value_of("file").unwrap();
    println!("Value of file: {:?}", db_path);

    let store = SledStore::open(db_path);
    Ok(())
}
