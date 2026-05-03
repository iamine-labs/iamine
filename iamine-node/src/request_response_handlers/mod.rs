use super::*;
use event_loop_control::EventLoopDirective;

mod request_path;
mod response_path;

pub(super) struct RequestResponseHandlerContext<'a> {
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) event: RREvent<TaskRequest, TaskResponse>,
    pub(super) peer_id: PeerId,
    pub(super) debug_flags: DebugFlags,
    pub(super) is_client: bool,
    pub(super) topology: &'a SharedNetworkTopology,
    pub(super) queue: &'a Arc<TaskQueue>,
    pub(super) task_manager: &'a Arc<TaskManager>,
    pub(super) task_response_tx: &'a tokio::sync::mpsc::Sender<PendingTaskResponse>,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) inference_backend_state: &'a InferenceBackendState,
    pub(super) infer_runtime: &'a mut InferRuntimeState,
    pub(super) client_state: &'a mut ClientRuntimeState,
}

pub(super) async fn handle_request_response_event(
    ctx: RequestResponseHandlerContext<'_>,
) -> EventLoopDirective {
    let RequestResponseHandlerContext {
        swarm,
        event,
        peer_id,
        debug_flags,
        is_client,
        topology,
        queue,
        task_manager,
        task_response_tx,
        registry,
        model_storage,
        inference_backend_state,
        infer_runtime,
        client_state,
    } = ctx;

    match event {
        RREvent::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
        } => {
            return request_path::handle_request_message(request_path::RequestMessageContext {
                swarm,
                peer,
                request,
                channel,
                peer_id,
                debug_flags,
                topology,
                queue,
                task_manager,
                task_response_tx,
                model_storage,
                inference_backend_state,
            })
            .await;
        }
        RREvent::Message {
            peer,
            message: Message::Response { response, .. },
        } => {
            return response_path::handle_response_message(response_path::ResponseMessageContext {
                swarm,
                peer,
                response,
                infer_runtime,
                client_state,
                task_manager,
                registry,
            })
            .await;
        }
        RREvent::OutboundFailure { peer, error, .. } => {
            eprintln!("❌ Outbound {}: {:?}", peer, error);
            if is_client && !client_state.waiting_for_response {
                return EventLoopDirective::Break;
            }
        }
        _ => {}
    }

    EventLoopDirective::None
}
