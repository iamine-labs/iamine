use super::*;

pub(super) fn should_use_ephemeral_identity(mode: &NodeMode) -> bool {
    matches!(mode, NodeMode::Infer { .. })
}

pub(super) fn worker_port_from_args(args: &[String]) -> u16 {
    args.iter()
        .find_map(|arg| arg.strip_prefix("--port=")?.parse::<u16>().ok())
        .unwrap_or(9000)
}

pub(super) fn listen_address_for_mode(
    mode: &NodeMode,
    worker_port: u16,
) -> Result<Multiaddr, std::io::Error> {
    let addr = if matches!(mode, NodeMode::Worker) {
        format!("/ip4/0.0.0.0/tcp/{}", worker_port)
    } else if matches!(mode, NodeMode::Relay) {
        "/ip4/0.0.0.0/tcp/9999".to_string()
    } else {
        "/ip4/0.0.0.0/tcp/0".to_string()
    };

    addr.parse::<Multiaddr>().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid listen address '{}': {}", addr, error),
        )
    })
}
