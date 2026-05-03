use std::time::Duration;

pub(super) async fn simulate_workers(count: usize, _base_peer_id: String) {
    println!("🔥 Iniciando simulación de {} workers...", count);
    let mut handles = vec![];
    for index in 0..count {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            let mut tasks_done = 0u64;
            loop {
                interval.tick().await;
                tasks_done += 1;
                if tasks_done.is_multiple_of(10) {
                    println!("👷 Worker-{}: {} tareas simuladas", index, tasks_done);
                }
            }
        });
        handles.push(handle);
    }
    println!(
        "✅ {} workers simulados activos. Ctrl+C para detener.",
        count
    );
    tokio::time::sleep(Duration::from_secs(60)).await;
}
