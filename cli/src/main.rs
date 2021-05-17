use anyhow::Error;
use oxigraph::io::DatasetFormat;
use std::path;

mod cli;

fn main() {
    match run_cli() {
        Ok(_) => {}
        Err(err) => eprintln!("Error: {}", err),
    }
}

fn run_cli() -> Result<(), Error> {
    use oxigraph::SledStore;
    use std::fs;
    use std::io;

    let matches = cli::build_cli().get_matches();

    let db_path = matches.value_of("file").unwrap();
    println!("Value of file: {:?}", db_path);

    let store = SledStore::open(db_path)?;

    if let Some(matches) = matches.subcommand_matches("load") {
        let data = path::Path::new(matches.value_of("data").unwrap());
        if let Some(format) = dataset_format_from_path(data) {
            store.load_dataset(io::BufReader::new(fs::File::open(data)?), format, None)?;
        }
    }
    Ok(())
}

fn dataset_format_from_path(path: &path::Path) -> Option<DatasetFormat> {
    match path.extension() {
        None => None,
        Some(ext) => {
            if ext == DatasetFormat::TriG.file_extension() {
                Some(DatasetFormat::TriG)
            } else if ext == DatasetFormat::NQuads.file_extension() {
                Some(DatasetFormat::NQuads)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod dataset_format {
        use super::*;

        #[test]
        fn no_extension() {
            assert_eq!(dataset_format_from_path(path::Path::new("test/test")), None);
        }

        #[test]
        fn unknown_extension() {
            assert_eq!(
                dataset_format_from_path(path::Path::new("test/test.foo")),
                None
            );
        }

        #[test]
        fn trig() {
            assert_eq!(
                dataset_format_from_path(path::Path::new("test/test.trig")),
                Some(DatasetFormat::TriG)
            );
        }

        #[test]
        fn nquads() {
            assert_eq!(
                dataset_format_from_path(path::Path::new("test/test.nq")),
                Some(DatasetFormat::NQuads)
            );
        }
    }
}
