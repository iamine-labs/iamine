use libp2p::{identity, PeerId};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct NodeIdentity {
    pub node_id: String,
    pub peer_id: PeerId,
    pub keypair: identity::Keypair,
    pub public_key: Vec<u8>,
    pub wallet_address: String,
}

impl NodeIdentity {
    pub fn load_or_create() -> Self {
        Self::load_or_create_with_announce(true)
    }

    pub fn load_or_create_quiet() -> Self {
        Self::load_or_create_with_announce(false)
    }

    fn load_or_create_with_announce(announce: bool) -> Self {
        match load_or_create_from_path(&default_identity_key_path(), announce) {
            Ok(identity) => identity,
            Err(error) => {
                eprintln!("node identity unavailable: {error}");
                std::process::exit(1);
            }
        }
    }

    pub fn ephemeral(reason: &str) -> Self {
        let keypair = identity::Keypair::generate_ed25519();
        let identity = Self::from_keypair(keypair);

        println!("Identidad efimera generada:");
        println!("   Peer ID: {}", identity.peer_id);
        println!("   Wallet:  {}", identity.wallet_address);
        println!("   Reason:  {}", reason);

        identity
    }

    fn from_keypair(keypair: identity::Keypair) -> Self {
        let peer_id = PeerId::from(keypair.public());
        let public_key = keypair.public().encode_protobuf();
        let peer_id_text = peer_id.to_string();
        let wallet_suffix: String = peer_id_text.chars().take(16).collect();
        let wallet_address = format!("iamine1{wallet_suffix}");

        Self {
            node_id: peer_id_text,
            peer_id,
            keypair,
            public_key,
            wallet_address,
        }
    }

    pub(crate) fn public_key_fingerprint(&self) -> String {
        sha256_hex(&self.public_key)
    }
}

fn load_or_create_from_path(path: &Path, announce: bool) -> Result<NodeIdentity, String> {
    let created = !path.exists();
    let identity = if created {
        create_identity_at_path(path)?
    } else {
        let identity = load_identity_from_path(path)?;
        let _ = repair_key_permissions(path)?;
        identity
    };

    if announce {
        if created {
            println!("Identidad nueva generada en ~/.iamine/node_key");
        }
        println!("Identidad cargada:");
        println!("   Peer ID: {}", identity.peer_id);
        println!("   Wallet:  {}", identity.wallet_address);
    }

    Ok(identity)
}

pub(crate) fn create_identity_at_path(path: &Path) -> Result<NodeIdentity, String> {
    let keypair = identity::Keypair::generate_ed25519();
    let bytes = keypair
        .to_protobuf_encoding()
        .map_err(|error| format!("serializing node identity key: {error}"))?;
    write_key_bytes(path, &bytes)?;
    Ok(NodeIdentity::from_keypair(keypair))
}

pub(crate) fn load_identity_from_path(path: &Path) -> Result<NodeIdentity, String> {
    let bytes = fs::read(path).map_err(|error| format!("reading node identity key: {error}"))?;
    let keypair = identity::Keypair::from_protobuf_encoding(&bytes)
        .map_err(|error| format!("parsing node identity key: {error}"))?;
    Ok(NodeIdentity::from_keypair(keypair))
}

fn write_key_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("creating node identity directory: {error}"))?;
        set_dir_permissions(parent)
            .map_err(|error| format!("setting node identity directory permissions: {error}"))?;
    }

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_create_mode_private(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| format!("creating node identity key: {error}"))?;
    file.write_all(bytes)
        .map_err(|error| format!("writing node identity key: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("syncing node identity key: {error}"))?;
    set_file_permissions(path)
        .map_err(|error| format!("setting node identity key permissions: {error}"))?;
    Ok(())
}

pub(crate) fn default_identity_key_path() -> PathBuf {
    iamine_dir().join("node_key")
}

pub fn iamine_dir() -> PathBuf {
    match dirs::home_dir() {
        Some(home) => home.join(".iamine"),
        None => PathBuf::from(".").join(".iamine"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(hex_char(byte >> 4));
        encoded.push(hex_char(byte & 0x0f));
    }
    encoded
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        _ => (b'a' + (value - 10)) as char,
    }
}

pub(crate) fn key_permissions_private(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        match fs::metadata(path) {
            Ok(metadata) => metadata.permissions().mode() & 0o077 == 0,
            Err(_) => false,
        }
    }

    #[cfg(not(unix))]
    {
        path.exists()
    }
}

pub(crate) fn repair_key_permissions(path: &Path) -> Result<bool, String> {
    if key_permissions_private(path) {
        return Ok(false);
    }
    set_file_permissions(path)
        .map_err(|error| format!("setting node identity key permissions: {error}"))?;
    Ok(true)
}

#[cfg(unix)]
fn set_create_mode_private(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_create_mode_private(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_file_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_dir_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_dir_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}
