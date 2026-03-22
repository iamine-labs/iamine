pub mod errors;
pub mod message;
pub mod node;
pub mod result;
pub mod task;

pub use errors::{IaMineError, IaMineResult};
pub use message::IaMineMessage;
pub use node::{NodeCapabilities, NodeReputation};
pub use result::TaskResult;
pub use task::{Task, TaskStatus, TaskType};

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::new().chain_update(data).finalize())
}

pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::new().chain_update(data).finalize().into()
}
