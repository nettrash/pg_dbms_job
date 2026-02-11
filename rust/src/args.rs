use crate::constants::PROGRAM;
use std::env;

#[derive(Default)]
pub struct Args {
    pub config_file: String,
    pub debug_override: Option<bool>,
    pub help: bool,
    pub kill: bool,
    pub abort: bool,
    pub reload: bool,
    pub single: bool,
    pub version: bool,
}

pub fn usage(config_file: &str) {
    println!(
        "usage: {PROGRAM} [options]\n\noptions:\n\n  -c, --config  file  configuration file. Default: {config_file}\n  -d, --debug         run in debug mode.\n  -k, --kill          stop current running daemon gracefully waiting\n                      for all job completion.\n  -m, --immediate     stop running daemon and jobs immediatly.\n  -r, --reload        reload configuration file and jobs definition.\n  -s, --single        do not detach and run in single loop mode and exit.\n"
    );
}

pub fn parse_args() -> Args {
    let argv: Vec<String> = env::args().skip(1).collect();
    parse_args_from(&argv)
}

fn parse_args_from(argv: &[String]) -> Args {
    let mut args = Args::default();
    let mut iter = argv.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                if let Some(val) = iter.next() {
                    args.config_file = val.to_string();
                }
            }
            "-d" | "--debug" => args.debug_override = Some(true),
            "--no-debug" => args.debug_override = Some(false),
            "-h" | "--help" => args.help = true,
            "-k" | "--kill" => args.kill = true,
            "-m" | "--immediate" => args.abort = true,
            "-r" | "--reload" => args.reload = true,
            "-s" | "--single" => args.single = true,
            "-v" | "--version" => args.version = true,
            _ => {}
        }
    }
    args
}

#[cfg(test)]
mod tests {
    use super::parse_args_from;

    #[test]
    fn parse_args_from_sets_flags() {
        let argv = vec![
            "--config".to_string(),
            "/tmp/test.conf".to_string(),
            "--debug".to_string(),
            "--single".to_string(),
        ];
        let args = parse_args_from(&argv);
        assert_eq!(args.config_file, "/tmp/test.conf");
        assert_eq!(args.debug_override, Some(true));
        assert!(args.single);
    }
}
