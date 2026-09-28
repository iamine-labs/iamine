use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use super::PHOTO_LIBRARY_INPUT_SCHEMA_VERSION;

pub(crate) const MAX_PHOTO_LIBRARY_ITEMS: usize = 8;
pub(crate) const MAX_PHOTO_LIBRARY_TOKEN_BYTES: usize = 40;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhotoLibraryCliCommand {
    pub(crate) package_root: String,
    pub(crate) items: Vec<PhotoInventoryEvidence>,
    pub(crate) intent: Option<PhotoOrganizationIntent>,
    pub(crate) json: bool,
}

impl PhotoLibraryCliCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let mut package_root = None;
        let mut items = Vec::new();
        let mut intent = None;
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
                "--item" => {
                    index += 1;
                    let token = parse_value(args.get(index), "--item")?;
                    items.push(PhotoInventoryEvidence::from_str(&token)?);
                }
                "--intent" => {
                    if intent.is_some() {
                        return Err("--intent no puede repetirse".to_string());
                    }
                    index += 1;
                    let token = parse_value(args.get(index), "--intent")?;
                    intent = Some(PhotoOrganizationIntent::from_str(&token)?);
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
                argument if argument.starts_with("--item=") => {
                    let token = parse_inline_value(argument, "--item=")?;
                    items.push(PhotoInventoryEvidence::from_str(&token)?);
                }
                argument if argument.starts_with("--intent=") => {
                    if intent.is_some() {
                        return Err("--intent no puede repetirse".to_string());
                    }
                    let token = parse_inline_value(argument, "--intent=")?;
                    intent = Some(PhotoOrganizationIntent::from_str(&token)?);
                }
                argument => {
                    return Err(format!(
                        "Argumento Photo Library Organizer no reconocido: {argument}"
                    ));
                }
            }
            if items.len() > MAX_PHOTO_LIBRARY_ITEMS {
                return Err(format!(
                    "Photo Library Organizer acepta maximo {MAX_PHOTO_LIBRARY_ITEMS} items"
                ));
            }
            index += 1;
        }

        validate_items(&items)?;
        Ok(Self {
            package_root: package_root.ok_or("Falta --package-root PATH")?,
            items,
            intent,
            json,
        })
    }

    pub(crate) fn input(&self) -> PhotoLibraryInput {
        PhotoLibraryInput {
            schema_version: PHOTO_LIBRARY_INPUT_SCHEMA_VERSION.to_string(),
            items: self.items.clone(),
            intent: self.intent,
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

fn bounded_token<'a>(value: &'a str, label: &str) -> Result<&'a str, String> {
    if value.is_empty() {
        return Err(format!("Falta {label} en --item"));
    }
    if value.len() > MAX_PHOTO_LIBRARY_TOKEN_BYTES {
        return Err(format!("{label} excede la longitud permitida"));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhotoLibraryInput {
    pub(crate) schema_version: String,
    pub(crate) items: Vec<PhotoInventoryEvidence>,
    pub(crate) intent: Option<PhotoOrganizationIntent>,
}

impl PhotoLibraryInput {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != PHOTO_LIBRARY_INPUT_SCHEMA_VERSION {
            return Err("schema Photo Library Organizer no compatible".to_string());
        }
        if self.items.len() > MAX_PHOTO_LIBRARY_ITEMS {
            return Err("demasiados items Photo Library Organizer".to_string());
        }
        validate_items(&self.items)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhotoInventoryEvidence {
    pub(crate) item: PhotoInventoryItemLabel,
    pub(crate) category: PhotoInventoryCategory,
    pub(crate) status: PhotoEvidenceStatus,
    pub(crate) claim: PhotoEvidenceClaim,
}

impl FromStr for PhotoInventoryEvidence {
    type Err = String;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        if token.len() > MAX_PHOTO_LIBRARY_TOKEN_BYTES * 4 {
            return Err(
                "Formato invalido para --item; use LABEL:CATEGORY:STATUS:CLAIM".to_string(),
            );
        }
        let mut parts = token.split(':');
        let item = bounded_token(parts.next().unwrap_or_default(), "label")?;
        let category = bounded_token(parts.next().unwrap_or_default(), "category")?;
        let status = bounded_token(parts.next().unwrap_or_default(), "status")?;
        let claim = bounded_token(parts.next().unwrap_or_default(), "claim")?;
        if parts.next().is_some() {
            return Err(
                "Formato invalido para --item; use LABEL:CATEGORY:STATUS:CLAIM".to_string(),
            );
        }
        Ok(Self {
            item: item.parse()?,
            category: category.parse()?,
            status: status.parse()?,
            claim: claim.parse()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum PhotoInventoryItemLabel {
    #[serde(rename = "personal_library")]
    Personal,
    #[serde(rename = "family_library")]
    Family,
    #[serde(rename = "travel_library")]
    Travel,
    #[serde(rename = "archive_library")]
    Archive,
}

impl FromStr for PhotoInventoryItemLabel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "personal_library" => Ok(Self::Personal),
            "family_library" => Ok(Self::Family),
            "travel_library" => Ok(Self::Travel),
            "archive_library" => Ok(Self::Archive),
            _ => Err("label Photo Library Organizer no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoInventoryCategory {
    Photo,
    Video,
    Screenshot,
    DocumentScan,
    UnknownItem,
}

impl FromStr for PhotoInventoryCategory {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "photo" => Ok(Self::Photo),
            "video" => Ok(Self::Video),
            "screenshot" => Ok(Self::Screenshot),
            "document_scan" => Ok(Self::DocumentScan),
            "unknown_item" => Ok(Self::UnknownItem),
            _ => Err("category Photo Library Organizer no permitida".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoEvidenceStatus {
    Observed,
    Attention,
    Blocked,
    Missing,
}

impl FromStr for PhotoEvidenceStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "observed" => Ok(Self::Observed),
            "attention" => Ok(Self::Attention),
            "blocked" => Ok(Self::Blocked),
            "missing" => Ok(Self::Missing),
            _ => Err("status Photo Library Organizer no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoEvidenceClaim {
    DeclaredInventoryBoundary,
    DeclaredMetadataCompleteness,
    DeclaredOrganizationIntent,
    DeclaredDuplicateSuspicion,
    UnsupportedClaim,
}

impl FromStr for PhotoEvidenceClaim {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "declared_inventory_boundary" => Ok(Self::DeclaredInventoryBoundary),
            "declared_metadata_completeness" => Ok(Self::DeclaredMetadataCompleteness),
            "declared_organization_intent" => Ok(Self::DeclaredOrganizationIntent),
            "declared_duplicate_suspicion" => Ok(Self::DeclaredDuplicateSuspicion),
            "unsupported_claim" => Ok(Self::UnsupportedClaim),
            _ => Err("claim Photo Library Organizer no permitido".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoOrganizationIntent {
    KeepAsIs,
    GroupByDeclaredCategory,
    ReviewBeforeChange,
    Undecided,
}

impl PhotoOrganizationIntent {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::KeepAsIs => "keep_as_is",
            Self::GroupByDeclaredCategory => "group_by_declared_category",
            Self::ReviewBeforeChange => "review_before_change",
            Self::Undecided => "undecided",
        }
    }
}

impl FromStr for PhotoOrganizationIntent {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "keep_as_is" => Ok(Self::KeepAsIs),
            "group_by_declared_category" => Ok(Self::GroupByDeclaredCategory),
            "review_before_change" => Ok(Self::ReviewBeforeChange),
            "undecided" => Ok(Self::Undecided),
            _ => Err("intent Photo Library Organizer no permitido".to_string()),
        }
    }
}

fn validate_items(items: &[PhotoInventoryEvidence]) -> Result<(), String> {
    let mut statuses = HashMap::with_capacity(items.len());
    for item in items {
        let key = (item.item, item.category, item.claim);
        if let Some(previous) = statuses.insert(key, item.status) {
            return if previous == item.status {
                Err("evidencia Photo Library Organizer duplicada".to_string())
            } else {
                Err("evidencia Photo Library Organizer contradictoria".to_string())
            };
        }
    }
    Ok(())
}
