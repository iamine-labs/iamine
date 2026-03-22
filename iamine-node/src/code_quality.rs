use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCheck {
    pub name: String,
    pub success: bool,
    pub warning_count: usize,
    pub error_count: usize,
    pub output_excerpt: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeQualityReport {
    pub format_passed: bool,
    pub clippy_passed: bool,
    pub warning_count: usize,
    pub commands: Vec<CommandCheck>,
}

impl CodeQualityReport {
    pub fn passed_for_minor(&self) -> bool {
        self.format_passed && self.clippy_passed
    }

    pub fn passed_for_major(&self) -> bool {
        self.passed_for_minor() && self.warning_count == 0
    }
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn run_code_quality_checks() -> Result<CodeQualityReport, String> {
    let root = workspace_root();
    let fmt = run_command(&root, "cargo", &["fmt", "--check"], "cargo fmt --check")?;
    let clippy = run_command(
        &root,
        "cargo",
        &["clippy", "-p", "iamine-node", "--all-targets"],
        "cargo clippy -p iamine-node --all-targets",
    )?;

    Ok(build_report(fmt, clippy))
}

fn build_report(fmt: CommandCheck, clippy: CommandCheck) -> CodeQualityReport {
    let warning_count = fmt.warning_count + clippy.warning_count;
    let format_passed = fmt.success;
    let clippy_passed = clippy.success || clippy.error_count == 0;

    CodeQualityReport {
        format_passed,
        clippy_passed,
        warning_count,
        commands: vec![fmt, clippy],
    }
}

fn run_command(
    cwd: &Path,
    program: &str,
    args: &[&str],
    name: &str,
) -> Result<CommandCheck, String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|error| format!("No se pudo ejecutar {}: {}", name, error))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}\n{}", stdout, stderr);

    Ok(CommandCheck {
        name: name.to_string(),
        success: output.status.success(),
        warning_count: count_warnings(&combined),
        error_count: count_errors(&combined),
        output_excerpt: excerpt(&combined, 12),
    })
}

fn count_warnings(output: &str) -> usize {
    output
        .lines()
        .filter(|line| line.trim_start().starts_with("warning:"))
        .count()
}

fn count_errors(output: &str) -> usize {
    output
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("error:") || trimmed.starts_with("error[")
        })
        .count()
}

fn excerpt(output: &str, max_lines: usize) -> String {
    output
        .lines()
        .take(max_lines)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_quality_check() {
        let fmt = CommandCheck {
            name: "cargo fmt --check".to_string(),
            success: true,
            warning_count: 0,
            error_count: 0,
            output_excerpt: String::new(),
        };
        let clippy = CommandCheck {
            name: "cargo clippy".to_string(),
            success: false,
            warning_count: 7,
            error_count: 0,
            output_excerpt: "warning: field is never read".to_string(),
        };

        let report = build_report(fmt, clippy);
        assert!(report.passed_for_minor());
        assert!(!report.passed_for_major());
        assert_eq!(report.warning_count, 7);
    }
}
