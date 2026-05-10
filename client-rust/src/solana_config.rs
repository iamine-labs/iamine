use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

pub const SOLANA_RPC_URL: &str = "https://api.devnet.solana.com";
pub const SOLANA_PROGRAM_ID: &str = "IAMINE1111111111111111111111111111111111";

pub fn get_program_id() -> Pubkey {
    Pubkey::from_str(SOLANA_PROGRAM_ID).expect("Invalid program ID")
}

// IDs de instrucciones del programa (deben coincidir con tu smart contract)
pub const INSTRUCTION_REGISTER_NODE: u8 = 0;
pub const INSTRUCTION_SUBMIT_CHALLENGE: u8 = 2;
pub const INSTRUCTION_CLAIM_REWARDS: u8 = 1;
