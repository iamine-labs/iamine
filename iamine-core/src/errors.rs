use thiserror::Error;

#[derive(Debug, Error)]
pub enum IaMineError {
    #[error("Tarea {0} no encontrada")]
    TaskNotFound(String),

    #[error("Tarea {0} expiró después de {1}ms")]
    TaskTimeout(String, u64),

    #[error("Worker {0} no puede manejar tarea tipo {1}")]
    UnsupportedTaskType(String, String),

    #[error("No hay workers disponibles para tarea tipo {0}")]
    NoWorkersAvailable(String),

    #[error("Worker pool lleno: {0}/{1} slots ocupados")]
    WorkerPoolFull(usize, usize),

    #[error("Máximo de reintentos alcanzado para tarea {0}")]
    MaxRetriesExceeded(String),

    #[error("Error de red: {0}")]
    NetworkError(String),

    #[error("Error de serialización: {0}")]
    SerializationError(String),

    #[error("Worker {0} tiene reputación insuficiente: {1}/100")]
    InsufficientReputation(String, u32),

    #[error("Hash de resultado inválido para tarea {0}")]
    InvalidResultHash(String),
}

pub type IaMineResult<T> = std::result::Result<T, IaMineError>;
