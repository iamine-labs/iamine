use crate::broadcast_protocol::build_task_bid_payload;
use crate::broadcast_worker::{
    emit_worker_task_bid_published_event, emit_worker_task_offer_received_event,
};
use crate::task_cache::TaskCache;
use crate::task_queue::TaskQueue;
use crate::worker_capabilities::WorkerCapabilities;
use crate::worker_pool::WorkerPool;
use crate::{log_observability_event, IamineBehaviour, BIDS_TOPIC};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use libp2p::{gossipsub, swarm::Swarm, PeerId};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerTaskOffer {
    pub(crate) task_id: String,
    pub(crate) task_type: String,
    pub(crate) data: String,
    pub(crate) origin_peer: String,
}

impl WorkerTaskOffer {
    pub(crate) fn from_value(value: &Value) -> Self {
        Self {
            task_id: value["task_id"].as_str().unwrap_or("").to_string(),
            task_type: value["task_type"].as_str().unwrap_or("").to_string(),
            data: value["data"].as_str().unwrap_or("").to_string(),
            origin_peer: value["origin_peer"].as_str().unwrap_or("").to_string(),
        }
    }
}

pub(crate) struct WorkerTaskOfferRuntimeContext<'a> {
    pub(crate) from_peer: &'a str,
    pub(crate) task_cache: &'a mut TaskCache,
    pub(crate) capabilities: &'a WorkerCapabilities,
    pub(crate) pool: &'a WorkerPool,
    pub(crate) queue: &'a TaskQueue,
    pub(crate) peer_id: &'a PeerId,
}

pub(crate) async fn handle_worker_task_offer(
    swarm: &mut Swarm<IamineBehaviour>,
    offer: WorkerTaskOffer,
    context: WorkerTaskOfferRuntimeContext<'_>,
) {
    emit_worker_task_offer_received_event(
        &offer.task_id,
        &offer.task_type,
        &offer.data,
        &offer.origin_peer,
        context.from_peer,
    );
    println!(
        "📥 [Worker] TaskOffer recibido: task_id={} type={} data='{}'",
        offer.task_id, offer.task_type, offer.data
    );

    if context.task_cache.is_duplicate(&offer.task_id) {
        println!(
            "⚡ [Cache] Tarea {} ya vista",
            &offer.task_id[..8.min(offer.task_id.len())]
        );
        return;
    }

    if !context.capabilities.supports(&offer.task_type) {
        println!("🚫 [Worker] No soporta '{}'", offer.task_type);
        return;
    }

    let available = context.pool.available_slots();
    if available == 0 {
        return;
    }

    println!("📋 [Worker] Bid para tarea {}", offer.task_id);
    let reputation_score = context.queue.reputation().await.reputation_score;
    let worker_id = context.peer_id.to_string();
    let bid = build_task_bid_payload(
        &offer.task_id,
        &worker_id,
        &offer.origin_peer,
        reputation_score,
        available,
        10,
    );
    match swarm.behaviour_mut().gossipsub.publish(
        gossipsub::IdentTopic::new(BIDS_TOPIC),
        serde_json::to_vec(&bid).unwrap_or_default(),
    ) {
        Ok(message_id) => emit_worker_task_bid_published_event(
            &offer.task_id,
            &worker_id,
            &message_id.to_string(),
            available,
        ),
        Err(error) => log_observability_event(
            LogLevel::Error,
            "task_bid_publish_failed",
            &offer.task_id,
            Some(&offer.task_id),
            None,
            Some(TASK_DISPATCH_UNCONFIRMED_001),
            {
                let mut fields = Map::new();
                fields.insert("worker_id".to_string(), worker_id.into());
                fields.insert("topic".to_string(), BIDS_TOPIC.into());
                fields.insert("error".to_string(), error.to_string().into());
                fields
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_pubsub_task_offer_preserves_fields() {
        let value = serde_json::json!({
            "type": "TaskOffer",
            "task_id": "task-1",
            "task_type": "reverse_string",
            "data": "abc",
            "origin_peer": "controller"
        });

        let offer = WorkerTaskOffer::from_value(&value);

        assert_eq!(offer.task_id, "task-1");
        assert_eq!(offer.task_type, "reverse_string");
        assert_eq!(offer.data, "abc");
        assert_eq!(offer.origin_peer, "controller");
    }
}
