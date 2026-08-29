use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use super::REPORTER_INPUT_SCHEMA_VERSION;

pub(crate) const MAX_REPORTER_EVIDENCE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReporterCliCommand {
    pub(crate) package_root: String,
    pub(crate) evidence: Vec<ReporterEvidence>,
    pub(crate) json: bool,
}

impl ReporterCliCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let mut package_root = None;
        let mut evidence = Vec::new();
        let mut json = false;
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--package-root" => {
                    if package_root.is_some() {
                        return Err("--package-root no puede repetirse".to_string());
                    }
                    index += 1;
                    package_root = Some(parse_value(args.get(index), "--package-root")?);
                }
                "--evidence" => {
                    index += 1;
                    let token = parse_value(args.get(index), "--evidence")?;
                    evidence.push(ReporterEvidence::from_str(&token)?);
                }
                "--json" => {
                    if json {
                        return Err("--json no puede repetirse".to_string());
                    }
                    json = true;
                }
                argument if argument.starts_with("--package-root=") => {
                    if package_root.is_some() {
                        return Err("--package-root no puede repetirse".to_string());
                    }
                    package_root = Some(parse_inline_value(argument, "--package-root=")?);
                }
                argument if argument.starts_with("--evidence=") => {
                    let token = parse_inline_value(argument, "--evidence=")?;
                    evidence.push(ReporterEvidence::from_str(&token)?);
                }
                argument => return Err(format!("Argumento Reporter no reconocido: {argument}")),
            }
            if evidence.len() > MAX_REPORTER_EVIDENCE {
                return Err(format!(
                    "Reporter acepta maximo {MAX_REPORTER_EVIDENCE} evidencias"
                ));
            }
            index += 1;
        }

        validate_evidence(&evidence)?;
        Ok(Self {
            package_root: package_root.ok_or("Falta --package-root PATH")?,
            evidence,
            json,
        })
    }

    pub(crate) fn input(&self) -> ReporterInput {
        ReporterInput {
            schema_version: REPORTER_INPUT_SCHEMA_VERSION.to_string(),
            evidence: self.evidence.clone(),
        }
    }
}

fn parse_value(value: Option<&String>, flag: &str) -> Result<String, String> {
    let value = value.ok_or_else(|| format!("Falta valor para {flag}"))?;
    if value.is_empty() || value.starts_with("--") || value.trim() != value {
        return Err(format!("Valor invalido para {flag}"));
    }
    Ok(value.clone())
}

fn parse_inline_value(argument: &str, prefix: &str) -> Result<String, String> {
    let value = argument.strip_prefix(prefix).unwrap_or_default();
    if value.is_empty() || value.trim() != value {
        return Err(format!(
            "Valor invalido para {}",
            prefix.trim_end_matches('=')
        ));
    }
    Ok(value.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReporterInput {
    pub(crate) schema_version: String,
    pub(crate) evidence: Vec<ReporterEvidence>,
}

impl ReporterInput {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != REPORTER_INPUT_SCHEMA_VERSION {
            return Err("schema Reporter no compatible".to_string());
        }
        if self.evidence.len() > MAX_REPORTER_EVIDENCE {
            return Err("demasiadas evidencias Reporter".to_string());
        }
        validate_evidence(&self.evidence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReporterEvidence {
    pub(crate) source: ReporterEvidenceSource,
    pub(crate) status: ReporterEvidenceStatus,
    pub(crate) claim: ReporterClaim,
}

impl FromStr for ReporterEvidence {
    type Err = String;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let mut parts = token.split(':');
        let source = parts.next().ok_or("Falta source en --evidence")?;
        let status = parts.next().ok_or("Falta status en --evidence")?;
        let claim = parts.next().ok_or("Falta claim en --evidence")?;
        if parts.next().is_some() || source.is_empty() || status.is_empty() || claim.is_empty() {
            return Err("Formato invalido para --evidence; use SOURCE:STATUS:CLAIM".to_string());
        }
        Ok(Self {
            source: source.parse()?,
            status: status.parse()?,
            claim: claim.parse()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum ReporterEvidenceSource {
    #[serde(rename = "operator_symptom_summary")]
    OperatorSymptom,
    #[serde(rename = "redacted_diagnostic_summary")]
    RedactedDiagnostic,
    #[serde(rename = "redacted_support_bundle_summary")]
    RedactedSupportBundle,
}

impl FromStr for ReporterEvidenceSource {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "operator_symptom_summary" => Ok(Self::OperatorSymptom),
            "redacted_diagnostic_summary" => Ok(Self::RedactedDiagnostic),
            "redacted_support_bundle_summary" => Ok(Self::RedactedSupportBundle),
            _ => Err("source Reporter no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReporterEvidenceStatus {
    Observed,
    Attention,
    Blocked,
    Missing,
}

impl FromStr for ReporterEvidenceStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "observed" => Ok(Self::Observed),
            "attention" => Ok(Self::Attention),
            "blocked" => Ok(Self::Blocked),
            "missing" => Ok(Self::Missing),
            _ => Err("status Reporter no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReporterClaim {
    NodeReadiness,
    ConfigurationStatus,
    ModelReadiness,
    NetworkReadiness,
    RuntimeHealth,
    UnsupportedClaim,
}

impl FromStr for ReporterClaim {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "node_readiness" => Ok(Self::NodeReadiness),
            "configuration_status" => Ok(Self::ConfigurationStatus),
            "model_readiness" => Ok(Self::ModelReadiness),
            "network_readiness" => Ok(Self::NetworkReadiness),
            "runtime_health" => Ok(Self::RuntimeHealth),
            "unsupported_claim" => Ok(Self::UnsupportedClaim),
            _ => Err("claim Reporter no permitido".to_string()),
        }
    }
}

fn validate_evidence(evidence: &[ReporterEvidence]) -> Result<(), String> {
    let mut statuses = HashMap::with_capacity(evidence.len());
    for item in evidence {
        let key = (item.source, item.claim);
        if let Some(previous) = statuses.insert(key, item.status) {
            return if previous == item.status {
                Err("evidencia Reporter duplicada".to_string())
            } else {
                Err("evidencia Reporter contradictoria".to_string())
            };
        }
    }
    Ok(())
}
