use iamine_network::{
    analyze_prompt_semantics, should_use_strict_handling, validate_semantic_decision, TaskType,
};
use std::fs;
use std::path::{Path, PathBuf};

const EXPECTED_VERSION_FILES: usize = 23;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionPromptCase {
    pub prompt: String,
    pub expected_task: TaskType,
    pub should_normalize: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSuite {
    pub version: String,
    pub cases: Vec<VersionPromptCase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegressionFailure {
    pub version: String,
    pub prompt: String,
    pub expected_task: TaskType,
    pub predicted_task: TaskType,
    pub expected_normalize: bool,
    pub predicted_normalize: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRegressionResult {
    pub version: String,
    pub total: usize,
    pub passed: usize,
    pub failures: Vec<RegressionFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegressionRunReport {
    pub total_versions: usize,
    pub total_prompts: usize,
    pub total_failures: usize,
    pub version_results: Vec<VersionRegressionResult>,
}

impl RegressionRunReport {
    pub fn passed(&self) -> bool {
        self.total_failures == 0
    }
}

pub fn default_suite_dir() -> PathBuf {
    workspace_root().join("version_test_suite")
}

pub fn run_default_regression_suite() -> Result<RegressionRunReport, String> {
    run_regression_suite(&default_suite_dir())
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn run_regression_suite(dir: &Path) -> Result<RegressionRunReport, String> {
    let suites = load_version_suites(dir)?;
    Ok(evaluate_regression_suites(&suites))
}

pub fn load_version_suites(dir: &Path) -> Result<Vec<VersionSuite>, String> {
    if !dir.exists() {
        return Err(format!(
            "No existe directorio de regression suites: {}",
            dir.display()
        ));
    }

    let mut paths = fs::read_dir(dir)
        .map_err(|error| format!("No se pudo leer {}: {}", dir.display(), error))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("txt"))
        .collect::<Vec<_>>();
    paths.sort_by_key(|path| version_sort_key(path));

    if paths.len() != EXPECTED_VERSION_FILES {
        return Err(format!(
            "Suites invalidas: se esperaban exactamente {} archivos y hay {}",
            EXPECTED_VERSION_FILES,
            paths.len()
        ));
    }

    let expected_files = (1..=EXPECTED_VERSION_FILES)
        .map(|version| format!("v0.6.{}.txt", version))
        .collect::<Vec<_>>();
    let found_files = paths
        .iter()
        .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
        .map(str::to_string)
        .collect::<Vec<_>>();

    if found_files != expected_files {
        return Err(format!(
            "Suites invalidas: se esperaban {:?} y se encontraron {:?}",
            expected_files, found_files
        ));
    }

    let mut suites = Vec::new();
    for path in paths {
        suites.push(load_version_suite(&path)?);
    }

    Ok(suites)
}

fn version_sort_key(path: &Path) -> (u64, u64, u64) {
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return (u64::MAX, u64::MAX, u64::MAX);
    };
    let raw = stem.strip_prefix('v').unwrap_or(stem);
    let mut parts = raw.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    let patch = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    (major, minor, patch)
}

pub fn load_version_suite(path: &Path) -> Result<VersionSuite, String> {
    let version = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| format!("Nombre de suite invalido: {}", path.display()))?
        .to_string();
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("No se pudo leer {}: {}", path.display(), error))?;
    let mut cases = Vec::new();

    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts = line.split('|').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(format!(
                "Linea invalida en {}:{} => '{}'",
                path.display(),
                index + 1,
                raw_line
            ));
        }

        cases.push(VersionPromptCase {
            prompt: parts[0].to_string(),
            expected_task: parse_task_type(parts[1])?,
            should_normalize: parse_bool(parts[2])?,
        });
    }

    if cases.is_empty() {
        return Err(format!("Suite vacia: {}", path.display()));
    }

    Ok(VersionSuite { version, cases })
}

pub fn evaluate_regression_suites(suites: &[VersionSuite]) -> RegressionRunReport {
    let mut version_results = Vec::new();
    let mut total_prompts = 0usize;
    let mut total_failures = 0usize;

    for suite in suites {
        let mut failures = Vec::new();
        let mut passed = 0usize;

        for case in &suite.cases {
            total_prompts += 1;
            let analyzed = analyze_prompt_semantics(&case.prompt);
            let validated = validate_semantic_decision(&case.prompt, analyzed);
            let predicted_task = validated.decision.profile.task_type;
            let predicted_normalize = should_use_strict_handling(predicted_task);

            if predicted_task == case.expected_task && predicted_normalize == case.should_normalize
            {
                passed += 1;
            } else {
                failures.push(RegressionFailure {
                    version: suite.version.clone(),
                    prompt: case.prompt.clone(),
                    expected_task: case.expected_task,
                    predicted_task,
                    expected_normalize: case.should_normalize,
                    predicted_normalize,
                });
            }
        }

        total_failures += failures.len();
        version_results.push(VersionRegressionResult {
            version: suite.version.clone(),
            total: suite.cases.len(),
            passed,
            failures,
        });
    }

    RegressionRunReport {
        total_versions: suites.len(),
        total_prompts,
        total_failures,
        version_results,
    }
}

fn parse_task_type(raw: &str) -> Result<TaskType, String> {
    match raw {
        "Math" => Ok(TaskType::Math),
        "ExactMath" => Ok(TaskType::ExactMath),
        "SymbolicMath" => Ok(TaskType::SymbolicMath),
        "Generative" => Ok(TaskType::Generative),
        "StructuredList" => Ok(TaskType::StructuredList),
        "Deterministic" => Ok(TaskType::Deterministic),
        "Code" => Ok(TaskType::Code),
        "Conceptual" => Ok(TaskType::Conceptual),
        "Reasoning" => Ok(TaskType::Reasoning),
        "Summarization" => Ok(TaskType::Summarization),
        "General" => Ok(TaskType::General),
        other => Err(format!(
            "TaskType desconocido en regression suite: {}",
            other
        )),
    }
}

fn parse_bool(raw: &str) -> Result<bool, String> {
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("Bool invalido en regression suite: {}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-regression-suite-{}", nanos))
    }

    #[test]
    fn test_regression_runner() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        for version in 1..=23 {
            fs::write(
                dir.join(format!("v0.6.{}.txt", version)),
                if version == 1 {
                    "What is 2+2? | ExactMath | true\n"
                } else {
                    "explica la teoria de la relatividad | Conceptual | false\n"
                },
            )
            .unwrap();
        }

        let report = run_regression_suite(&dir).unwrap();
        assert_eq!(report.total_versions, 23);
        assert_eq!(report.total_failures, 0);
        assert!(report.passed());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_regression_runner_requires_complete_version_set() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("v0.6.1.txt"), "What is 2+2? | ExactMath | true\n").unwrap();

        let error = run_regression_suite(&dir).unwrap_err();
        assert!(error.contains("exactamente 23 archivos"));
        let _ = fs::remove_dir_all(dir);
    }
}
