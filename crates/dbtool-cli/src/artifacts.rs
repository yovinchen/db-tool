use clap::{Arg, Command};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

pub fn write_cli_artifacts(mut command: Command, out_dir: &Path) -> std::io::Result<()> {
    command.build();

    let completions = out_dir.join("completions");
    let man = out_dir.join("man");
    fs::create_dir_all(&completions)?;
    fs::create_dir_all(&man)?;

    fs::write(completions.join("dbtool.bash"), bash_completion(&command))?;
    fs::write(completions.join("dbtool.zsh"), zsh_completion(&command))?;
    fs::write(completions.join("dbtool.fish"), fish_completion(&command))?;
    fs::write(man.join("dbtool.1"), manpage(&command))?;

    Ok(())
}

fn bash_completion(command: &Command) -> String {
    let paths = command_paths(command);
    let mut out = String::from(
        r#"# bash completion for dbtool
_dbtool_subcommands_for() {
  case "$1" in
"#,
    );

    for path in &paths {
        push_case_line(&mut out, &path.key, &path.subcommands);
    }

    out.push_str(
        r#"    *) echo "" ;;
  esac
}

_dbtool_candidates_for() {
  case "$1" in
"#,
    );

    for path in &paths {
        push_case_line(&mut out, &path.key, &path.candidates);
    }

    out.push_str(
        r#"    *) echo "" ;;
  esac
}

_dbtool() {
  local cur key token subs candidates
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  key=""

  for ((i = 1; i < COMP_CWORD; i++)); do
    token="${COMP_WORDS[i]}"
    [[ "$token" == -* ]] && continue
    subs="$(_dbtool_subcommands_for "$key")"
    if [[ " $subs " == *" $token "* ]]; then
      if [[ -z "$key" ]]; then
        key="$token"
      else
        key="$key $token"
      fi
    fi
  done

  candidates="$(_dbtool_candidates_for "$key")"
  COMPREPLY=( $(compgen -W "$candidates" -- "$cur") )
  return 0
}

complete -F _dbtool dbtool
"#,
    );

    out
}

fn zsh_completion(command: &Command) -> String {
    let root_commands = visible_subcommands(command)
        .into_iter()
        .map(|name| shell_word(&name))
        .collect::<Vec<_>>()
        .join(" ");
    let mut options = option_specs(command)
        .into_iter()
        .map(|option| zsh_option_spec(&option))
        .collect::<Vec<_>>();
    options.push("'1:command:(".to_owned() + &root_commands + ")'");
    options.push("'*::argument:->args'".to_owned());

    let mut out = String::from("#compdef dbtool\n\n_dbtool() {\n  _arguments -C \\\n");
    for (idx, option) in options.iter().enumerate() {
        let suffix = if idx + 1 == options.len() { "" } else { " \\" };
        out.push_str("    ");
        out.push_str(option);
        out.push_str(suffix);
        out.push('\n');
    }
    out.push_str("}\n\n_dbtool \"$@\"\n");
    out
}

fn fish_completion(command: &Command) -> String {
    let paths = command_paths(command);
    let mut out = String::from("# fish completion for dbtool\n");

    for option in option_specs(command) {
        out.push_str(&fish_option_line(&option, None));
    }

    let root_subcommands = visible_subcommands(command);
    let root_condition = format!(
        "not __fish_seen_subcommand_from {}",
        root_subcommands.join(" ")
    );
    for subcommand in command.get_subcommands().filter(|cmd| !cmd.is_hide_set()) {
        out.push_str(&format!(
            "complete -c dbtool -f -n {} -a {} -d {}\n",
            fish_quote(&root_condition),
            fish_quote(subcommand.get_name()),
            fish_quote(&about(subcommand))
        ));
    }

    for path in paths.into_iter().filter(|path| !path.path.is_empty()) {
        for option in &path.local_options {
            out.push_str(&fish_option_line(option, Some(&path.path)));
        }
        for subcommand in &path.subcommands {
            let condition = fish_path_condition(&path.path);
            out.push_str(&format!(
                "complete -c dbtool -f -n {} -a {}\n",
                fish_quote(&condition),
                fish_quote(subcommand)
            ));
        }
    }

    out
}

fn manpage(command: &Command) -> String {
    let name = command.get_name();
    let mut usage_command = command.clone();
    let usage = usage_command.render_usage().to_string();
    let mut out = format!(
        ".TH {} 1 \"2026-06-16\" \"dbtool {}\" \"User Commands\"\n",
        roff_escape(&name.to_ascii_uppercase()),
        roff_escape(env!("CARGO_PKG_VERSION"))
    );

    out.push_str(".SH NAME\n");
    out.push_str(&format!(
        "{} \\- {}\n",
        roff_escape(name),
        roff_escape(&about(command))
    ));
    out.push_str(".SH SYNOPSIS\n");
    out.push_str(&format!("{}\n", roff_escape(&usage)));
    out.push_str(".SH DESCRIPTION\n");
    out.push_str(&format!("{}\n", roff_escape(&about(command))));
    out.push_str(".SH GLOBAL OPTIONS\n");
    for option in option_specs(command) {
        out.push_str(".TP\n");
        out.push_str(&format!(".B {}\n", roff_escape(&option.display)));
        out.push_str(&format!("{}\n", roff_escape(&option.help)));
    }
    out.push_str(".SH COMMANDS\n");
    append_man_commands(&mut out, command, Vec::new());
    out.push_str(".SH FILES\n");
    out.push_str(".TP\n");
    out.push_str(".B completions/dbtool.bash\n");
    out.push_str("Bash completion script included in release archives and wrapper packages.\n");
    out.push_str(".TP\n");
    out.push_str(".B completions/dbtool.zsh\n");
    out.push_str("Zsh completion script included in release archives and wrapper packages.\n");
    out.push_str(".TP\n");
    out.push_str(".B completions/dbtool.fish\n");
    out.push_str("Fish completion script included in release archives and wrapper packages.\n");

    out
}

fn append_man_commands(out: &mut String, command: &Command, path: Vec<String>) {
    for subcommand in command.get_subcommands().filter(|cmd| !cmd.is_hide_set()) {
        let mut sub_path = path.clone();
        sub_path.push(subcommand.get_name().to_owned());
        out.push_str(".TP\n");
        out.push_str(&format!(".B dbtool {}\n", roff_escape(&sub_path.join(" "))));
        out.push_str(&format!("{}\n", roff_escape(&about(subcommand))));

        let local_options = option_specs(subcommand);
        if !local_options.is_empty() {
            out.push_str("Options: ");
            out.push_str(&roff_escape(
                &local_options
                    .iter()
                    .map(|option| option.display.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
            out.push('\n');
        }

        append_man_commands(out, subcommand, sub_path);
    }
}

#[derive(Debug)]
struct PathInfo {
    path: Vec<String>,
    key: String,
    subcommands: Vec<String>,
    local_options: Vec<OptionSpec>,
    candidates: Vec<String>,
}

#[derive(Clone, Debug)]
struct OptionSpec {
    short: Option<char>,
    long: Option<String>,
    display: String,
    help: String,
}

fn command_paths(command: &Command) -> Vec<PathInfo> {
    let global_options = option_specs(command);
    let mut paths = Vec::new();
    collect_command_paths(command, Vec::new(), &global_options, &mut paths);
    paths
}

fn collect_command_paths(
    command: &Command,
    path: Vec<String>,
    global_options: &[OptionSpec],
    paths: &mut Vec<PathInfo>,
) {
    let subcommands = visible_subcommands(command);
    let local_options = option_specs(command);
    let mut candidates = BTreeSet::new();

    for option in global_options {
        for name in option_names(option) {
            candidates.insert(name);
        }
    }
    for option in &local_options {
        for name in option_names(option) {
            candidates.insert(name);
        }
    }
    for subcommand in &subcommands {
        candidates.insert(subcommand.clone());
    }
    candidates.insert("--help".to_owned());
    if path.is_empty() {
        candidates.insert("--version".to_owned());
    }

    paths.push(PathInfo {
        key: path.join(" "),
        path: path.clone(),
        subcommands,
        local_options,
        candidates: candidates.into_iter().collect(),
    });

    for subcommand in command.get_subcommands().filter(|cmd| !cmd.is_hide_set()) {
        let mut sub_path = path.clone();
        sub_path.push(subcommand.get_name().to_owned());
        collect_command_paths(subcommand, sub_path, global_options, paths);
    }
}

fn visible_subcommands(command: &Command) -> Vec<String> {
    command
        .get_subcommands()
        .filter(|cmd| !cmd.is_hide_set())
        .map(|cmd| cmd.get_name().to_owned())
        .collect()
}

fn option_specs(command: &Command) -> Vec<OptionSpec> {
    command
        .get_arguments()
        .filter(|arg| !arg.is_hide_set() && !arg.is_positional())
        .filter_map(option_spec)
        .collect()
}

fn option_spec(arg: &Arg) -> Option<OptionSpec> {
    let short = arg.get_short();
    let long = arg.get_long().map(ToOwned::to_owned);
    if short.is_none() && long.is_none() {
        return None;
    }

    let mut names = Vec::new();
    if let Some(short) = short {
        names.push(format!("-{short}"));
    }
    if let Some(long) = &long {
        names.push(format!("--{long}"));
    }

    Some(OptionSpec {
        short,
        long,
        display: names.join(", "),
        help: arg
            .get_help()
            .or_else(|| arg.get_long_help())
            .map(ToString::to_string)
            .unwrap_or_default(),
    })
}

fn option_names(option: &OptionSpec) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(short) = option.short {
        names.push(format!("-{short}"));
    }
    if let Some(long) = &option.long {
        names.push(format!("--{long}"));
    }
    names
}

fn about(command: &Command) -> String {
    command
        .get_about()
        .or_else(|| command.get_long_about())
        .map(ToString::to_string)
        .unwrap_or_default()
}

fn push_case_line(out: &mut String, key: &str, values: &[String]) {
    out.push_str("    ");
    out.push_str(&bash_case_pattern(key));
    out.push_str(") echo ");
    out.push_str(&bash_single_quote(&values.join(" ")));
    out.push_str(" ;;\n");
}

fn bash_case_pattern(key: &str) -> String {
    if key.is_empty() {
        "\"\"".to_owned()
    } else {
        bash_single_quote(key)
    }
}

fn bash_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn zsh_option_spec(option: &OptionSpec) -> String {
    let spec = if let (Some(short), Some(long)) = (option.short, &option.long) {
        format!("{{-{short},--{long}}}")
    } else if let Some(short) = option.short {
        format!("-{short}")
    } else if let Some(long) = &option.long {
        format!("--{long}")
    } else {
        return String::new();
    };
    format!(
        "'{}[{}]'",
        spec.replace('\'', "'\\''"),
        option.help.replace('\'', "'\\''")
    )
}

fn fish_option_line(option: &OptionSpec, path: Option<&[String]>) -> String {
    let mut line = "complete -c dbtool".to_owned();
    if let Some(path) = path {
        line.push_str(" -n ");
        line.push_str(&fish_quote(&fish_path_condition(path)));
    }
    if let Some(short) = option.short {
        line.push_str(&format!(" -s {short}"));
    }
    if let Some(long) = &option.long {
        line.push_str(" -l ");
        line.push_str(long);
    }
    if !option.help.is_empty() {
        line.push_str(" -d ");
        line.push_str(&fish_quote(&option.help));
    }
    line.push('\n');
    line
}

fn fish_path_condition(path: &[String]) -> String {
    if path.is_empty() {
        return "true".to_owned();
    }
    path.iter()
        .map(|part| format!("__fish_seen_subcommand_from {part}"))
        .collect::<Vec<_>>()
        .join("; and ")
}

fn shell_word(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect()
}

fn fish_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn roff_escape(value: &str) -> String {
    let mut escaped = String::new();
    for line in value.lines() {
        if line.starts_with('.') || line.starts_with('\'') {
            escaped.push_str("\\&");
        }
        escaped.push_str(&line.replace('\\', "\\e").replace('-', "\\-"));
        escaped.push('\n');
    }
    escaped.trim_end().to_owned()
}

pub fn artifact_paths(out_dir: &Path) -> [PathBuf; 4] {
    [
        out_dir.join("completions/dbtool.bash"),
        out_dir.join("completions/dbtool.zsh"),
        out_dir.join("completions/dbtool.fish"),
        out_dir.join("man/dbtool.1"),
    ]
}
