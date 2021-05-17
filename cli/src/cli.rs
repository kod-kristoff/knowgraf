use clap::{App, Arg, SubCommand};

pub fn build_cli() -> App<'static, 'static> {
    use clap::{crate_authors, crate_version};
    App::new("knowgraf-cli")
        .version(crate_version!())
        .author(crate_authors!(","))
        .about("Command-line interface to knowgraf.")
        .arg(
            Arg::with_name("file")
                .short("f")
                .long("file")
                .value_name("PATH")
                .help("Specify the db")
                .required(true),
        )
        .subcommand(
            SubCommand::with_name("query").about("query the graf").arg(
                Arg::with_name("query")
                    .short("q")
                    .long("query")
                    .value_name("INPUT"),
            ),
        )
        .arg(
            Arg::with_name("update")
                .short("u")
                .long("update")
                .value_name("INPUT"),
        )
        .subcommand(
            SubCommand::with_name("load").arg(
                Arg::with_name("data")
                    .short("d")
                    .long("data")
                    .value_name("INPUT"),
            ),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_is_required() {
        let args = vec!["kg-cli"];
        let m = build_cli().get_matches_from_safe(args);

        match m {
            Err(_) => {}
            _ => panic!("should not be here."),
        }
    }
}
