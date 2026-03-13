use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::sync::Arc;

pub struct SolanaReporter {
    pub keypair: Arc<Keypair>,
    pub pubkey: Pubkey,
    rpc_url: String,
}

impl SolanaReporter {
    pub fn new(keypair: Keypair, rpc_url: &str) -> Self {
        let pubkey = keypair.pubkey();
        Self {
            keypair: Arc::new(keypair),
            pubkey,
            rpc_url: rpc_url.to_string(),
        }
    }

    pub fn devnet() -> Self {
        Self::new(
            Keypair::new(),
            "https://api.devnet.solana.com",
        )
    }

    /// Reportar tarea completada (simula tx en devnet)
    pub async fn report_task(&self, task_id: &str, result_hash: &[u8; 32]) -> Result<String, String> {
        let client = RpcClient::new_with_commitment(
            self.rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );

        // Verificar saldo
        match client.get_balance(&self.pubkey) {
            Ok(balance) => {
                println!("   💳 Saldo worker: {} lamports", balance);
                // TODO: llamar al programa Anchor con submit_challenge_response
                // Por ahora simulamos el tx hash
                let fake_tx = format!("TX_{}_{}",
                    &task_id[..std::cmp::min(8, task_id.len())],
                    hex::encode(&result_hash[..4])
                );
                Ok(fake_tx)
            }
            Err(e) => Err(format!("Sin conexión Solana: {}", e)),
        }
    }
}
