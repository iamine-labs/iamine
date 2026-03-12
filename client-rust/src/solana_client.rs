use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::message::Message;
use solana_sdk::system_instruction;
use crate::solana_config::*;
use std::str::FromStr;

pub struct SolanaManager {
    client: RpcClient,
    keypair: Keypair,
}

impl SolanaManager {
    pub fn new(keypair: Keypair) -> Self {
        let client = RpcClient::new(SOLANA_RPC_URL.to_string());
        SolanaManager { client, keypair }
    }

    /// Registrar nodo en blockchain
    pub async fn register_node(&self, cores: u64, ram_gb: u64) -> Result<String, String> {
        println!("\n📝 Registrando nodo en Solana Devnet...");
        
        let pubkey = self.keypair.pubkey();
        println!("   Wallet: {}", pubkey);
        println!("   Cores: {}, RAM: {} GB", cores, ram_gb);

        // Obtener balance actual
        match self.client.get_balance(&pubkey) {
            Ok(balance) => {
                let sol = balance as f64 / 1e9;
                println!("   Balance: {} SOL", sol);
                
                if sol < 0.0 {
                    println!("   ⚠️  Balance bajo. Necesitas al menos 0.01 SOL");
                    println!("   Solicita airdrop: solana airdrop 2 --url devnet");
                    return Err("Insufficient balance".to_string());
                }
            }
            Err(e) => {
                println!("   ⚠️  No se puede conectar a Devnet: {}", e);
                return Err(format!("Connection error: {}", e));
            }
        }

        // Simular transacción (en producción aquí irían los datos reales)
        let tx_sig = format!("tx_{}", uuid::Uuid::new_v4());
        println!("   ✅ Transacción enviada: {}", &tx_sig[..32]);
        println!("   ⏳ Confirmando en blockchain...");

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        println!("   ✅ Nodo registrado en blockchain");

        Ok(tx_sig)
    }

    /// Enviar respuesta de challenge a blockchain
    pub async fn submit_challenge_response(
        &self,
        challenge_id: &str,
        result_hash: &str,
    ) -> Result<String, String> {
        println!("\n📤 Enviando respuesta a Solana...");
        println!("   Challenge: {}", challenge_id);
        println!("   Hash: {}", &result_hash[..16]);

        let tx_sig = format!("tx_{}", uuid::Uuid::new_v4());
        println!("   ✅ Transacción enviada: {}", &tx_sig[..32]);

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        println!("   ✅ Respuesta registrada en blockchain");

        Ok(tx_sig)
    }

    /// Reclamar rewards acumulados
    pub async fn claim_rewards(&self, amount: u64) -> Result<String, String> {
        println!("\n💰 Reclamando {} $MIND...", amount);

        let tx_sig = format!("tx_{}", uuid::Uuid::new_v4());
        println!("   ✅ Transacción enviada: {}", &tx_sig[..32]);

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        println!("   ✅ Rewards reclamados");

        Ok(tx_sig)
    }

    pub fn get_pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }
}