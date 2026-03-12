use anchor_lang::prelude::*;

declare_id!("IAMINE1111111111111111111111111111111111"); // placeholder, lo cambiamos en mainnet

#[program]
pub mod iamine {
    use super::*;

    pub fn register_node(ctx: Context<RegisterNode>, compute_units: u64, ram_gb: u64) -> Result<()> {
        let node = &mut ctx.accounts.node;
        node.owner = ctx.accounts.user.key();
        node.compute_units = compute_units; // ej: 2
        node.ram_gb = ram_gb;               // ej: 2
        node.reputation = 100;
        node.is_active = true;
        Ok(())
    }
}

#[account]
pub struct Node {
    pub owner: Pubkey,
    pub compute_units: u64,
    pub ram_gb: u64,
    pub reputation: u32,
    pub is_active: bool,
}

#[derive(Accounts)]
pub struct RegisterNode<'info> {
    #[account(init, payer = user, space = 8 + 32 + 8 + 8 + 4 + 1)]
    pub node: Account<'info, Node>,
    #[account(mut)]
    pub user: Signer<'info>,
    pub system_program: Program<'info, System>,
}
