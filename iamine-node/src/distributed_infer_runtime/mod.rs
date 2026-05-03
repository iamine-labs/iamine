use super::*;
use event_loop_control::EventLoopDirective;

mod dispatch_phase;
mod timeout_phase;

pub(super) struct DistributedInferTickContext<'a> {
    pub(super) mode: &'a NodeMode,
    pub(super) infer_runtime: &'a mut InferRuntimeState,
    pub(super) model_storage: &'a ModelStorage,
    pub(super) topology: &'a SharedNetworkTopology,
    pub(super) peer_id: PeerId,
    pub(super) registry: &'a SharedNodeRegistry,
    pub(super) pubsub_topics: &'a PubsubTopicTracker,
    pub(super) client_state: &'a mut ClientRuntimeState,
    pub(super) debug_flags: DebugFlags,
    pub(super) swarm: &'a mut Swarm<IamineBehaviour>,
    pub(super) metrics: &'a Arc<RwLock<NodeMetrics>>,
    pub(super) task_manager: &'a Arc<TaskManager>,
}

pub(super) async fn handle_distributed_infer_tick(
    ctx: DistributedInferTickContext<'_>,
) -> Result<EventLoopDirective, Box<dyn Error>> {
    let DistributedInferTickContext {
        mode,
        infer_runtime,
        model_storage,
        topology,
        peer_id,
        registry,
        pubsub_topics,
        client_state,
        debug_flags,
        swarm,
        metrics,
        task_manager,
    } = ctx;

    match dispatch_phase::handle_dispatch_phase(dispatch_phase::DispatchPhaseContext {
        mode,
        infer_runtime,
        model_storage,
        topology,
        peer_id,
        registry,
        pubsub_topics,
        client_state,
        debug_flags,
        swarm,
        metrics,
    })
    .await?
    {
        EventLoopDirective::None => {}
        EventLoopDirective::Continue => return Ok(EventLoopDirective::Continue),
        EventLoopDirective::Break => return Ok(EventLoopDirective::Break),
    }

    timeout_phase::handle_timeout_phase(timeout_phase::TimeoutPhaseContext {
        mode,
        infer_runtime,
        registry,
        task_manager,
        client_state,
    })
    .await
}
