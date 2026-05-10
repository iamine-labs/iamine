use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecuritySeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityFinding {
    pub path: String,
    pub line: usize,
    pub severity: SecuritySeverity,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityReport {
    pub passed: bool,
    pub warning_count: usize,
    pub error_count: usize,
    pub findings: Vec<SecurityFinding>,
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn run_security_checks() -> Result<SecurityReport, String> {
    scan_security_paths(&workspace_root())
}

pub fn scan_security_paths(root: &Path) -> Result<SecurityReport, String> {
    let mut findings = Vec::new();
    scan_dir(root, root, &mut findings)?;

    let warning_count = findings
        .iter()
        .filter(|finding| matches!(finding.severity, SecuritySeverity::Warning))
        .count();
    let error_count = findings
        .iter()
        .filter(|finding| matches!(finding.severity, SecuritySeverity::Error))
        .count();

    Ok(SecurityReport {
        passed: error_count == 0,
        warning_count,
        error_count,
        findings,
    })
}

fn scan_dir(
    root: &Path,
    current: &Path,
    findings: &mut Vec<SecurityFinding>,
) -> Result<(), String> {
    let entries = fs::read_dir(current)
        .map_err(|error| format!("No se pudo leer {}: {}", current.display(), error))?;

    for entry in entries {
        let entry = entry.map_err(|error| format!("Error leyendo entrada: {}", error))?;
        let path = entry.path();

        if should_skip(&path) {
            continue;
        }

        if path.is_dir() {
            scan_dir(root, &path, findings)?;
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            scan_file(root, &path, findings)?;
        }
    }

    Ok(())
}

fn should_skip(path: &Path) -> bool {
    let skip_names = ["target", ".git", "tests"];
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| skip_names.contains(&name))
        .unwrap_or(false)
}

fn scan_file(root: &Path, path: &Path, findings: &mut Vec<SecurityFinding>) -> Result<(), String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("No se pudo leer {}: {}", path.display(), error))?;
    let relative = path
        .strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string();

    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.starts_with("//") {
            continue;
        }
        let normalized = strip_string_literals(line);

        if normalized.contains("unsafe fn") || normalized.contains("unsafe {") {
            findings.push(SecurityFinding {
                path: relative.clone(),
                line: index + 1,
                severity: SecuritySeverity::Error,
                message: "unsafe usage requires review".to_string(),
            });
        }

        if normalized.contains(".unwrap()")
            || normalized.contains(".expect(")
            || normalized.contains("panic!(")
        {
            findings.push(SecurityFinding {
                path: relative.clone(),
                line: index + 1,
                severity: SecuritySeverity::Warning,
                message: "panic-prone path detected".to_string(),
            });
        }
    }

    Ok(())
}

fn strip_string_literals(line: &str) -> String {
    let mut normalized = String::with_capacity(line.len());
    let mut in_string = false;
    let mut escaped = false;

    for ch in line.chars() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }

            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            continue;
        }

        normalized.push(ch);
    }

    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_security_checks() {
        let dir = tempdir().unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("sample.rs"),
            "fn main() {\n    let _x = value.unwrap();\n}\n",
        )
        .unwrap();

        let report = scan_security_paths(dir.path()).unwrap();
        assert!(report.passed);
        assert_eq!(report.warning_count, 1);
        assert_eq!(report.error_count, 0);
    }

    #[test]
    fn test_security_check_ignores_string_literals() {
        let dir = tempdir().unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("sample.rs"),
            "fn main() {\n    let _text = \"unsafe fn demo()\";\n}\n",
        )
        .unwrap();

        let report = scan_security_paths(dir.path()).unwrap();
        assert!(report.passed);
        assert_eq!(report.error_count, 0);
    }
}
