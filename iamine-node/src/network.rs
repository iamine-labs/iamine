use crate::protocol::IaMineMessage;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IaMineProtocol;

// Marcador para protocolo simple
// En futuros pasos, implementaremos request-response correctamente

#[allow(dead_code)]
impl IaMineProtocol {
    /// Serializar mensaje a JSON bytes con length prefix
    pub fn encode(msg: &IaMineMessage) -> Result<Vec<u8>, String> {
        let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
        let len = json.len() as u32;
        let mut result = len.to_be_bytes().to_vec();
        result.extend(json.into_bytes());
        Ok(result)
    }

    /// Deserializar mensaje de JSON bytes
    pub fn decode(data: &[u8]) -> Result<IaMineMessage, String> {
        if data.len() < 4 {
            return Err("Data too short".to_string());
        }
        let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let json = std::str::from_utf8(&data[4..4 + len]).map_err(|e| e.to_string())?;
        serde_json::from_str(json).map_err(|e| e.to_string())
    }
}
