use libp2p::{identity, PeerId};
use std::fs;
use std::path::PathBuf;

pub struct NodeIdentity {
    pub node_id: String,
    pub peer_id: PeerId,
    pub keypair: identity::Keypair,
    pub public_key: Vec<u8>,
    pub wallet_address: String,
}

impl NodeIdentity {
    pub fn load_or_create() -> Self {
        let dir = iamine_dir();
        fs::create_dir_all(&dir).expect("No se pudo crear ~/.iamine");

        let key_path = dir.join("node_key");

        let keypair = if key_path.exists() {
            let bytes = fs::read(&key_path).expect("Error leyendo node_key");
            identity::Keypair::from_protobuf_encoding(&bytes)
                .expect("node_key corrupto — borra ~/.iamine/node_key")
        } else {
            let kp = identity::Keypair::generate_ed25519();
            let bytes = kp.to_protobuf_encoding().expect("Error serializando keypair");
            fs::write(&key_path, &bytes).expect("Error guardando node_key");
            println!("🆕 Identidad nueva generada y guardada en ~/.iamine/node_key");
            kp
        };

        let peer_id = PeerId::from(keypair.public());
        let public_key = keypair.public().encode_protobuf();
        let wallet_address = format!("iamine1{}", &peer_id.to_string()[..16]);

        println!("🔑 Identidad cargada:");
        println!("   Peer ID: {}", peer_id);
        println!("   Wallet:  {}", wallet_address);

        Self {
            node_id: peer_id.to_string(),
            peer_id,
            keypair,
            public_key,
            wallet_address,
        }
    }
}

pub fn iamine_dir() -> PathBuf {
    let home = dirs::home_dir().expect("No se encontró home dir");
    home.join(".iamine")
}
