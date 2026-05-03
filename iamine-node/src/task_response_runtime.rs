use super::*;

pub(super) fn handle_completed_task_response(
    swarm: &mut Swarm<IamineBehaviour>,
    completed_response: PendingTaskResponse,
) {
    let task_id = completed_response.response.task_id.clone();
    if swarm
        .behaviour_mut()
        .request_response
        .send_response(completed_response.channel, completed_response.response)
        .is_ok()
    {
        println!("[Task] Completed {}", task_id);
    } else {
        eprintln!("[Task] Failed to return result for {}", task_id);
    }
}
