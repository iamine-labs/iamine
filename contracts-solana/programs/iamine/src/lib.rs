use anchor_lang::prelude::*;

declare_id!("IAMINE1111111111111111111111111111111111");

#[program]
pub mod iamine {
    use super::*;

    pub fn register_node(ctx: Context<RegisterNode>, compute_units: u64, ram_gb: u64) -> Result<()> {
        let node = &mut ctx.accounts.node;
        node.owner = ctx.accounts.user.key();
        node.compute_units = compute_units;
        node.ram_gb = ram_gb;
        node.reputation = 100;
        node.is_active = true;
        node.total_earned = 0;
        node.tasks_completed = 0;
        node.created_at = Clock::get()?.unix_timestamp;
        node.last_claim = 0;
        Ok(())
    }

    pub fn claim_rewards(ctx: Context<ClaimRewards>, amount: u64) -> Result<()> {
        let node = &mut ctx.accounts.node;
        
        require!(ctx.accounts.user.key() == node.owner, CustomError::Unauthorized);
        require!(node.is_active, CustomError::NodeInactive);
        require!(amount > 0, CustomError::InvalidAmount);
        
        node.total_earned += amount;
        node.last_claim = Clock::get()?.unix_timestamp;
        
        emit!(RewardsClaimed {
            node_owner: node.owner,
            amount,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn report_task_completed(ctx: Context<ReportTaskCompleted>) -> Result<()> {
        let node = &mut ctx.accounts.node;
        
        require!(ctx.accounts.user.key() == node.owner, CustomError::Unauthorized);
        require!(node.is_active, CustomError::NodeInactive);
        
        node.tasks_completed += 1;
        
        emit!(TaskCompleted {
            node_owner: node.owner,
            total_tasks: node.tasks_completed,
        });
        
        Ok(())
    }

    pub fn update_reputation(ctx: Context<UpdateReputation>, new_reputation: u32) -> Result<()> {
        let node = &mut ctx.accounts.node;
        
        require!(ctx.accounts.user.key() == node.owner, CustomError::Unauthorized);
        require!(new_reputation <= 100, CustomError::InvalidReputation);
        
        node.reputation = new_reputation;
        
        emit!(ReputationUpdated {
            node_owner: node.owner,
            new_reputation,
        });
        
        Ok(())
    }

    pub fn deactivate_node(ctx: Context<DeactivateNode>) -> Result<()> {
        let node = &mut ctx.accounts.node;
        
        require!(ctx.accounts.user.key() == node.owner, CustomError::Unauthorized);
        
        node.is_active = false;
        
        emit!(NodeDeactivated {
            node_owner: node.owner,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
        Ok(())
    }

    // 🆕 Crear un challenge (tarea) para un nodo
    pub fn create_challenge(
        ctx: Context<CreateChallenge>,
        challenge_data: Vec<u8>,
        expected_hash: [u8; 32],
        reward_amount: u64,
    ) -> Result<()> {
        let challenge = &mut ctx.accounts.challenge;
        
        challenge.id = ctx.accounts.challenge.key();
        challenge.node = ctx.accounts.node.key();
        challenge.creator = ctx.accounts.creator.key();
        challenge.challenge_data = challenge_data;
        challenge.expected_hash = expected_hash;
        challenge.reward_amount = reward_amount;
        challenge.status = ChallengeStatus::Pending;
        challenge.created_at = Clock::get()?.unix_timestamp;
        challenge.submitted_hash = None;
        challenge.is_valid = false;
        
        emit!(ChallengeCreated {
            challenge_id: challenge.id,
            node: challenge.node,
            reward: reward_amount,
        });
        
        Ok(())
    }

    // 🆕 Nodo envía su respuesta
    pub fn submit_challenge_response(
        ctx: Context<SubmitChallengeResponse>,
        submitted_hash: [u8; 32],
    ) -> Result<()> {
        let challenge = &mut ctx.accounts.challenge;
        let node = &mut ctx.accounts.node;
        
        require!(challenge.status == ChallengeStatus::Pending, CustomError::ChallengeNotPending);
        require!(ctx.accounts.user.key() == node.owner, CustomError::Unauthorized);
        require!(challenge.node == node.key(), CustomError::WrongNode);
        
        challenge.submitted_hash = Some(submitted_hash);
        challenge.status = ChallengeStatus::Submitted;
        challenge.submitted_at = Some(Clock::get()?.unix_timestamp);
        
        emit!(ChallengeSubmitted {
            challenge_id: challenge.id,
            node: node.key(),
        });
        
        Ok(())
    }

    // 🆕 Validador verifica la respuesta
    pub fn validate_challenge(ctx: Context<ValidateChallenge>) -> Result<()> {
        let challenge = &mut ctx.accounts.challenge;
        let node = &mut ctx.accounts.node;
        
        require!(challenge.status == ChallengeStatus::Submitted, CustomError::ChallengeNotSubmitted);
        require!(challenge.submitted_hash.is_some(), CustomError::NoSubmissionFound);
        
        let submitted = challenge.submitted_hash.unwrap();
        
        if submitted == challenge.expected_hash {
            // ✅ Respuesta correcta
            challenge.status = ChallengeStatus::Valid;
            challenge.is_valid = true;
            
            // Recompensa + aumenta reputación
            node.total_earned += challenge.reward_amount;
            node.reputation = std::cmp::min(100, node.reputation + 5);
            node.tasks_completed += 1;
            
            emit!(ChallengeValidated {
                challenge_id: challenge.id,
                node: node.key(),
                reward: challenge.reward_amount,
            });
        } else {
            // ❌ Respuesta incorrecta - SLASH
            challenge.status = ChallengeStatus::Invalid;
            challenge.is_valid = false;
            
            // Penalización
            node.reputation = node.reputation.saturating_sub(10);
            
            if node.reputation == 0 {
                node.is_active = false;
            }
            
            emit!(ChallengeInvalid {
                challenge_id: challenge.id,
                node: node.key(),
            });
        }
        
        Ok(())
    }
}

// 🆕 Estados del challenge
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq)]
pub enum ChallengeStatus {
    Pending,   // Espera respuesta
    Submitted, // Nodo respondió
    Valid,     // Respuesta correcta
    Invalid,   // Respuesta incorrecta
}

// 🆕 Struct para guardar challenges
#[account]
pub struct Challenge {
    pub id: Pubkey,
    pub node: Pubkey,
    pub creator: Pubkey,
    pub challenge_data: Vec<u8>,
    pub expected_hash: [u8; 32],
    pub submitted_hash: Option<[u8; 32]>,
    pub reward_amount: u64,
    pub status: ChallengeStatus,
    pub is_valid: bool,
    pub created_at: i64,
    pub submitted_at: Option<i64>,
}

#[derive(Accounts)]
pub struct RegisterNode<'info> {
    #[account(init, payer = user, space = 8 + 32 + 8 + 8 + 4 + 1 + 8 + 8 + 8 + 8)]
    pub node: Account<'info, Node>,
    #[account(mut)]
    pub user: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct ClaimRewards<'info> {
    #[account(mut)]
    pub node: Account<'info, Node>,
    pub user: Signer<'info>,
}

#[derive(Accounts)]
pub struct ReportTaskCompleted<'info> {
    #[account(mut)]
    pub node: Account<'info, Node>,
    pub user: Signer<'info>,
}

#[derive(Accounts)]
pub struct UpdateReputation<'info> {
    #[account(mut)]
    pub node: Account<'info, Node>,
    pub user: Signer<'info>,
}

#[derive(Accounts)]
pub struct DeactivateNode<'info> {
    #[account(mut)]
    pub node: Account<'info, Node>,
    pub user: Signer<'info>,
}

#[derive(Accounts)]
pub struct CreateChallenge<'info> {
    #[account(init, payer = creator, space = 8 + 32 + 32 + 32 + 256 + 32 + 32 + 8 + 1 + 1 + 8 + 8 + 8)]
    pub challenge: Account<'info, Challenge>,
    pub node: Account<'info, Node>,
    #[account(mut)]
    pub creator: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct SubmitChallengeResponse<'info> {
    #[account(mut)]
    pub challenge: Account<'info, Challenge>,
    #[account(mut)]
    pub node: Account<'info, Node>,
    pub user: Signer<'info>,
}

#[derive(Accounts)]
pub struct ValidateChallenge<'info> {
    #[account(mut)]
    pub challenge: Account<'info, Challenge>,
    #[account(mut)]
    pub node: Account<'info, Node>,
}

#[event]
pub struct RewardsClaimed {
    pub node_owner: Pubkey,
    pub amount: u64,
    pub timestamp: i64,
}

#[event]
pub struct TaskCompleted {
    pub node_owner: Pubkey,
    pub total_tasks: u64,
}

#[event]
pub struct ReputationUpdated {
    pub node_owner: Pubkey,
    pub new_reputation: u32,
}

#[event]
pub struct NodeDeactivated {
    pub node_owner: Pubkey,
    pub timestamp: i64,
}

#[event]
pub struct ChallengeCreated {
    pub challenge_id: Pubkey,
    pub node: Pubkey,
    pub reward: u64,
}

#[event]
pub struct ChallengeSubmitted {
    pub challenge_id: Pubkey,
    pub node: Pubkey,
}

#[event]
pub struct ChallengeValidated {
    pub challenge_id: Pubkey,
    pub node: Pubkey,
    pub reward: u64,
}

#[event]
pub struct ChallengeInvalid {
    pub challenge_id: Pubkey,
    pub node: Pubkey,
}

#[error_code]
pub enum CustomError {
    #[msg("No autorizado para esta acción")]
    Unauthorized,
    #[msg("El nodo no está activo")]
    NodeInactive,
    #[msg("Cantidad de rewards inválida")]
    InvalidAmount,
    #[msg("Reputación debe estar entre 0-100")]
    InvalidReputation,
    #[msg("Challenge no está en estado Pending")]
    ChallengeNotPending,
    #[msg("Challenge no está en estado Submitted")]
    ChallengeNotSubmitted,
    #[msg("No se encontró submission")]
    NoSubmissionFound,
    #[msg("Este nodo no está asignado a este challenge")]
    WrongNode,
}
