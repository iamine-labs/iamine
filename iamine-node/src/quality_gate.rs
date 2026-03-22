use crate::code_quality::{run_code_quality_checks, CodeQualityReport};
use crate::regression_runner::{run_default_regression_suite, RegressionRunReport};
use crate::security_checks::{run_security_checks, SecurityReport};
use iamine_network::{evaluate_default_dataset, SemanticEvalReport};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CURRENT_RELEASE_VERSION: &str = "v0.6.24";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseTrack {
    Minor,
    Major,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCommandResult {
    pub name: String,
    pub success: bool,
    pub output_excerpt: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestsReport {
    pub passed: bool,
    pub commands: Vec<TestCommandResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseValidationSummary {
    pub version: String,
    pub track: ReleaseTrack,
    pub quality: QualityReport,
    pub tests: TestsReport,
    pub regression: RegressionRunReport,
    pub security: SecurityReport,
    pub code_quality: CodeQualityReport,
    pub semantic_eval: Option<SemanticEvalReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityReport {
    pub tests_passed: bool,
    pub regression_passed: bool,
    pub security_passed: bool,
    pub code_clean_passed: bool,
}

impl QualityReport {
    pub fn blocks_release(&self) -> bool {
        !(self.tests_passed
            && self.regression_passed
            && self.security_passed
            && self.code_clean_passed)
    }
}

pub fn current_release_version() -> &'static str {
    CURRENT_RELEASE_VERSION
}

pub fn run_release_validation() -> Result<ReleaseValidationSummary, String> {
    run_release_validation_for_version(CURRENT_RELEASE_VERSION)
}

pub fn run_release_validation_for_version(
    version: &str,
) -> Result<ReleaseValidationSummary, String> {
    let track = release_track(version);
    let tests = run_tests_report(track)?;
    let regression = run_default_regression_suite()?;
    let security = run_security_checks()?;
    let code_quality = run_code_quality_checks()?;
    let semantic_eval = if matches!(track, ReleaseTrack::Major) {
        Some(evaluate_default_dataset().map_err(|error| error.to_string())?)
    } else {
        None
    };

    let quality = build_quality_report(
        track,
        &tests,
        &regression,
        &security,
        &code_quality,
        semantic_eval.as_ref(),
    );

    Ok(ReleaseValidationSummary {
        version: version.to_string(),
        track,
        quality,
        tests,
        regression,
        security,
        code_quality,
        semantic_eval,
    })
}

pub fn build_quality_report(
    track: ReleaseTrack,
    tests: &TestsReport,
    regression: &RegressionRunReport,
    security: &SecurityReport,
    code_quality: &CodeQualityReport,
    semantic_eval: Option<&SemanticEvalReport>,
) -> QualityReport {
    let code_clean_passed = match track {
        ReleaseTrack::Minor => code_quality.passed_for_minor(),
        ReleaseTrack::Major => code_quality.passed_for_major(),
    };
    let regression_passed = regression.passed()
        && semantic_eval
            .map(|report| report.error_cases.is_empty())
            .unwrap_or(true);

    QualityReport {
        tests_passed: tests.passed,
        regression_passed,
        security_passed: security.passed,
        code_clean_passed,
    }
}

fn release_track(version: &str) -> ReleaseTrack {
    let core = version.strip_prefix('v').unwrap_or(version);
    let mut parts = core.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(0);

    if major >= 1 || minor >= 7 {
        ReleaseTrack::Major
    } else {
        ReleaseTrack::Minor
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn run_tests_report(track: ReleaseTrack) -> Result<TestsReport, String> {
    let root = workspace_root();
    let commands = match track {
        ReleaseTrack::Minor => vec![
            run_test_command(
                &root,
                "cargo test -p iamine-network",
                &["test", "-p", "iamine-network"],
            )?,
            run_test_command(
                &root,
                "cargo test -p iamine-models --lib",
                &["test", "-p", "iamine-models", "--lib"],
            )?,
            run_test_command(
                &root,
                "cargo test -p iamine-node -- --skip daemon_runtime::tests::test_daemon_start_stop",
                &[
                    "test",
                    "-p",
                    "iamine-node",
                    "--",
                    "--skip",
                    "daemon_runtime::tests::test_daemon_start_stop",
                ],
            )?,
        ],
        ReleaseTrack::Major => vec![
            run_test_command(
                &root,
                "cargo test -p iamine-network",
                &["test", "-p", "iamine-network"],
            )?,
            run_test_command(
                &root,
                "cargo test -p iamine-models",
                &["test", "-p", "iamine-models"],
            )?,
            run_test_command(
                &root,
                "cargo test -p iamine-node",
                &["test", "-p", "iamine-node"],
            )?,
        ],
    };

    Ok(TestsReport {
        passed: commands.iter().all(|command| command.success),
        commands,
    })
}

fn run_test_command(cwd: &Path, name: &str, args: &[&str]) -> Result<TestCommandResult, String> {
    let status = Command::new("cargo")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|error| format!("No se pudo ejecutar {}: {}", name, error))?;

    Ok(TestCommandResult {
        name: name.to_string(),
        success: status.success(),
        output_excerpt: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing_tests_report() -> TestsReport {
        TestsReport {
            passed: true,
            commands: Vec::new(),
        }
    }

    fn passing_regression_report() -> RegressionRunReport {
        RegressionRunReport {
            total_versions: 23,
            total_prompts: 23,
            total_failures: 0,
            version_results: Vec::new(),
        }
    }

    fn passing_security_report() -> SecurityReport {
        SecurityReport {
            passed: true,
            warning_count: 2,
            error_count: 0,
            findings: Vec::new(),
        }
    }

    fn passing_code_quality_report() -> CodeQualityReport {
        CodeQualityReport {
            format_passed: true,
            clippy_passed: true,
            warning_count: 4,
            commands: Vec::new(),
        }
    }

    #[test]
    fn test_quality_gate_pass() {
        let quality = build_quality_report(
            ReleaseTrack::Minor,
            &passing_tests_report(),
            &passing_regression_report(),
            &passing_security_report(),
            &passing_code_quality_report(),
            None,
        );

        assert!(!quality.blocks_release());
    }

    #[test]
    fn test_quality_gate_fail() {
        let mut security = passing_security_report();
        security.passed = false;
        security.error_count = 1;

        let quality = build_quality_report(
            ReleaseTrack::Minor,
            &passing_tests_report(),
            &passing_regression_report(),
            &security,
            &passing_code_quality_report(),
            None,
        );

        assert!(quality.blocks_release());
    }
}
