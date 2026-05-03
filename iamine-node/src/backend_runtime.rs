use super::*;

pub(super) async fn choose_inference_runtime() -> Option<InferenceRuntime> {
    let socket = daemon_socket_path();
    if daemon_is_available(&socket).await {
        println!(
            "[Daemon] Connected to persistent runtime: {}",
            socket.display()
        );
        return Some(InferenceRuntime::Daemon(socket));
    }
    None
}
