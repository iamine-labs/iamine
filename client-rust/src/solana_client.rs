use crate::solana_config::*;
use solana_client::rpc_client::RpcClient;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use std::cmp::min;
use std::str::FromStr;

pub struct SolanaManager {
    client: RpcClient,
    keypair: Keypair,
}

impl SolanaManager {
    const MEMO_PROGRAM_ID: &'static str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
    const MIN_BALANCE_SOL: f64 = 0.000_01;

    pub fn new(keypair: Keypair) -> Self {
        let client = RpcClient::new(SOLANA_RPC_URL.to_string());
        SolanaManager { client, keypair }
    }

    fn memo_instruction(&self, memo: &str) -> Result<Instruction, String> {
        let program_id = Pubkey::from_str(Self::MEMO_PROGRAM_ID)
            .map_err(|e| format!("Memo program id invalido: {}", e))?;
        Ok(Instruction {
            program_id,
            accounts: vec![AccountMeta::new_readonly(self.keypair.pubkey(), true)],
            data: memo.as_bytes().to_vec(),
        })
    }

    fn send_memo_transaction(&self, memo: &str) -> Result<String, String> {
        let ix = self.memo_instruction(memo)?;
        let recent_blockhash = self
            .client
            .get_latest_blockhash()
            .map_err(|e| format!("No se pudo obtener blockhash: {}", e))?;
        let message = Message::new(&[ix], Some(&self.keypair.pubkey()));
        let tx = Transaction::new(&[&self.keypair], message, recent_blockhash);

        self.client
            .send_and_confirm_transaction(&tx)
            .map(|sig| sig.to_string())
            .map_err(|e| format!("No se pudo confirmar transaccion: {}", e))
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

                if sol < Self::MIN_BALANCE_SOL {
                    println!(
                        "   ⚠️  Balance bajo. Necesitas al menos {} SOL",
                        Self::MIN_BALANCE_SOL
                    );
                    println!("   Solicita airdrop: solana airdrop 2 --url devnet");
                    return Err("Insufficient balance".to_string());
                }
            }
            Err(e) => {
                println!("   ⚠️  No se puede conectar a Devnet: {}", e);
                return Err(format!("Connection error: {}", e));
            }
        }

        let memo = serde_json::json!({
            "action": "register_node",
            "pubkey": pubkey.to_string(),
            "cores": cores,
            "ram_gb": ram_gb
        })
        .to_string();

        let tx_sig = self.send_memo_transaction(&memo)?;
        println!("   ✅ Transacción enviada: {}", tx_sig);
        println!("   ⏳ Confirmando en blockchain...");
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
        println!("   Hash: {}", &result_hash[..min(16, result_hash.len())]);

        let memo = serde_json::json!({
            "action": "submit_challenge_response",
            "pubkey": self.keypair.pubkey().to_string(),
            "challenge_id": challenge_id,
            "result_hash": result_hash
        })
        .to_string();
        let tx_sig = self.send_memo_transaction(&memo)?;
        println!("   ✅ Transacción enviada: {}", tx_sig);
        println!("   ✅ Respuesta registrada en blockchain");

        Ok(tx_sig)
    }

    /// Reclamar rewards acumulados
    pub async fn claim_rewards(&self, amount: u64) -> Result<String, String> {
        println!("\n💰 Reclamando {} $MIND...", amount);

        let memo = serde_json::json!({
            "action": "claim_rewards",
            "pubkey": self.keypair.pubkey().to_string(),
            "amount": amount
        })
        .to_string();
        let tx_sig = self.send_memo_transaction(&memo)?;
        println!("   ✅ Transacción enviada: {}", tx_sig);
        println!("   ✅ Rewards reclamados");

        Ok(tx_sig)
    }

    /// Reportar tarea completada
    pub async fn report_task_completed(&self) -> Result<String, String> {
        println!("\n✅ Reportando tarea completada a Solana...");

        let memo = serde_json::json!({
            "action": "report_task_completed",
            "pubkey": self.keypair.pubkey().to_string(),
        })
        .to_string();
        let tx_sig = self.send_memo_transaction(&memo)?;
        println!("   ✅ Transacción enviada: {}", tx_sig);
        println!("   ✅ Tarea reportada como completada");

        Ok(tx_sig)
    }

    pub fn get_pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }
}
