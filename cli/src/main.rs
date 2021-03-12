use anyhow::Error;

fn main() {
    match cli() {
        Ok(_) => {},
        Err(err) => eprintln!("Error: {}", err),
    }
}

fn cli() -> Result<(), Error> {
    use clap::{crate_version, Arg, App};
    use oxigraph::SledStore;

    let matches = App::new("knowgraf cli")
        .version(crate_version!())
        .author("Kristoffer Andersson <kod.kristoff@gmail.com>")
        .about("Command-line interface to knowgraf.")
        .arg(Arg::with_name("file")
                .short("f")
                .long("file")
                .value_name("PATH")
                .help("Specify the db")
                .required(true))
        .get_matches();

    let db_path = matches.value_of("file").unwrap();
    println!("Value of file: {:?}", db_path);

    let store = SledStore::open(db_path);
    Ok(())
}
