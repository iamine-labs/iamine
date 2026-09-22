use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use super::LAN_FILE_SHARE_INPUT_SCHEMA_VERSION;

pub(crate) const MAX_LAN_FILE_SHARE_EVIDENCE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LanFileShareCliCommand {
    pub(crate) package_root: String,
    pub(crate) shares: Vec<LanShareEvidence>,
    pub(crate) json: bool,
}

impl LanFileShareCliCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let mut package_root = None;
        let mut shares = Vec::new();
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
                "--share" => {
                    index += 1;
                    let token = parse_value(args.get(index), "--share")?;
                    shares.push(LanShareEvidence::from_str(&token)?);
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
                argument if argument.starts_with("--share=") => {
                    let token = parse_inline_value(argument, "--share=")?;
                    shares.push(LanShareEvidence::from_str(&token)?);
                }
                argument => {
                    return Err(format!(
                        "Argumento LAN file share no reconocido: {argument}"
                    ));
                }
            }
            if shares.len() > MAX_LAN_FILE_SHARE_EVIDENCE {
                return Err(format!(
                    "LAN file share acepta maximo {MAX_LAN_FILE_SHARE_EVIDENCE} shares"
                ));
            }
            index += 1;
        }

        validate_shares(&shares)?;
        Ok(Self {
            package_root: package_root.ok_or("Falta --package-root PATH")?,
            shares,
            json,
        })
    }

    pub(crate) fn input(&self) -> LanFileShareInput {
        LanFileShareInput {
            schema_version: LAN_FILE_SHARE_INPUT_SCHEMA_VERSION.to_string(),
            shares: self.shares.clone(),
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
pub(crate) struct LanFileShareInput {
    pub(crate) schema_version: String,
    pub(crate) shares: Vec<LanShareEvidence>,
}

impl LanFileShareInput {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != LAN_FILE_SHARE_INPUT_SCHEMA_VERSION {
            return Err("schema LAN file share no compatible".to_string());
        }
        if self.shares.len() > MAX_LAN_FILE_SHARE_EVIDENCE {
            return Err("demasiadas shares LAN file share".to_string());
        }
        validate_shares(&self.shares)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LanShareEvidence {
    pub(crate) share: ShareSelector,
    pub(crate) status: ShareEvidenceStatus,
    pub(crate) claim: ShareEvidenceClaim,
}

impl FromStr for LanShareEvidence {
    type Err = String;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let mut parts = token.split(':');
        let share = parts.next().ok_or("Falta share en --share")?;
        let status = parts.next().ok_or("Falta status en --share")?;
        let claim = parts.next().ok_or("Falta claim en --share")?;
        if parts.next().is_some() || share.is_empty() || status.is_empty() || claim.is_empty() {
            return Err("Formato invalido para --share; use SHARE:STATUS:CLAIM".to_string());
        }
        Ok(Self {
            share: share.parse()?,
            status: status.parse()?,
            claim: claim.parse()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum ShareSelector {
    #[serde(rename = "documents_share")]
    Documents,
    #[serde(rename = "media_share")]
    Media,
    #[serde(rename = "backup_share")]
    Backup,
    #[serde(rename = "team_share")]
    Team,
}

impl FromStr for ShareSelector {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "documents_share" => Ok(Self::Documents),
            "media_share" => Ok(Self::Media),
            "backup_share" => Ok(Self::Backup),
            "team_share" => Ok(Self::Team),
            _ => Err("share LAN no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShareEvidenceStatus {
    Observed,
    Attention,
    Blocked,
    Missing,
}

impl FromStr for ShareEvidenceStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "observed" => Ok(Self::Observed),
            "attention" => Ok(Self::Attention),
            "blocked" => Ok(Self::Blocked),
            "missing" => Ok(Self::Missing),
            _ => Err("status LAN file share no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShareEvidenceClaim {
    ReadonlyBoundary,
    ShareSelection,
    ProtocolMetadata,
    OwnerMetadata,
    UnsupportedClaim,
}

impl FromStr for ShareEvidenceClaim {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "readonly_boundary" => Ok(Self::ReadonlyBoundary),
            "share_selection" => Ok(Self::ShareSelection),
            "protocol_metadata" => Ok(Self::ProtocolMetadata),
            "owner_metadata" => Ok(Self::OwnerMetadata),
            "unsupported_claim" => Ok(Self::UnsupportedClaim),
            _ => Err("claim LAN file share no permitido".to_string()),
        }
    }
}

fn validate_shares(shares: &[LanShareEvidence]) -> Result<(), String> {
    let mut statuses = HashMap::with_capacity(shares.len());
    for item in shares {
        let key = (item.share, item.claim);
        if let Some(previous) = statuses.insert(key, item.status) {
            return if previous == item.status {
                Err("evidencia LAN file share duplicada".to_string())
            } else {
                Err("evidencia LAN file share contradictoria".to_string())
            };
        }
    }
    Ok(())
}
