mod solana_config;
mod solana_client;

use sysinfo::System;
use tokio::task;
use sha2::{Sha256, Digest};
use std::sync::Arc;
use tokio::sync::RwLock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use serde_json::json;
use std::convert::Infallible;
use warp::Filter;
use solana_client::SolanaManager;

#[derive(Clone, Debug)]
struct NodeState {
    pubkey: Pubkey,
    cores: u64,
    ram_gb: u64,
    reputation: u32,
    total_earned: u64,
    tasks_completed: u64,
}

#[derive(Clone, Debug)]
struct Challenge {
    id: String,
    data: String,
    expected_hash: String,
    reward: u64,
}

fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[tokio::main]
async fn main() {
    println!("🔥 IaMine Client iniciado");

    // 1️⃣ DETECTAR HARDWARE
    let mut sys = System::new_all();
    sys.refresh_all();
    let cores = sys.cpus().len() as u64;
    let ram_gb = sys.total_memory() / 1_073_741_824;

    println!("📊 Hardware detectado:");
    println!("   Cores: {}", cores);
    println!("   RAM: {} GB", ram_gb);

    // 2️⃣ CARGAR KEYPAIR SOLANA
    let keypair = Keypair::new();
    let node_pubkey = keypair.pubkey();
    println!("\n🔐 Identidad Solana:");
    println!("   Wallet: {}", node_pubkey);

    // 3️⃣ CREAR MANAGER SOLANA
    let solana_manager = Arc::new(SolanaManager::new(keypair));

    // 4️⃣ REGISTRAR NODO EN BLOCKCHAIN
    match solana_manager.register_node(cores, ram_gb).await {
        Ok(tx_sig) => println!("   ✅ Nodo registrado. Tx: {}\n", tx_sig),
        Err(e) => {
            println!("   ⚠️ Sin conexión Solana (modo local): {}\n", e);
        }
    }

    // 5️⃣ CREAR ESTADO DEL NODO
    let node_state = Arc::new(RwLock::new(NodeState {
        pubkey: node_pubkey,
        cores,
        ram_gb,
        reputation: 100,
        total_earned: 0,
        tasks_completed: 0,
    }));

    // 6️⃣ INICIAR SERVIDOR WEB EN PUERTO 3000
    println!("🌐 Dashboard en http://localhost:3000");
    let web_state = Arc::clone(&node_state);
    task::spawn(async move {
        start_web_server(web_state).await;
    });

    // 7️⃣ GENERAR Y EJECUTAR CHALLENGES LOCALMENTE
    // (sin TCP — usa el sistema P2P del iamine-node)
    println!("⚡ Generando challenges...\n");

    let sm = Arc::clone(&solana_manager);
    let ns = Arc::clone(&node_state);

    let mut counter = 1u64;
    loop {
        let challenge = Challenge {
            id: format!("challenge_{:03}", counter),
            data: format!("task_data_{}", counter),
            expected_hash: compute_sha256(format!("task_data_{}", counter).as_bytes()),
            reward: 50 + counter * 10,
        };

        println!("🔵 Challenge {}: data='{}' reward={} $MIND",
            challenge.id, challenge.data, challenge.reward);

        // Ejecutar localmente (simular worker)
        let computed = compute_sha256(challenge.data.as_bytes());
        let success = computed == challenge.expected_hash;

        if success {
            println!("✅ Challenge {} validado correctamente", challenge.id);

            // Actualizar estado local
            {
                let mut state = ns.write().await;
                state.tasks_completed += 1;
                state.total_earned += challenge.reward;
                println!("💰 Total ganado: {} $MIND | Tareas: {}",
                    state.total_earned, state.tasks_completed);
            }

            // Reportar a Solana
            match sm.report_task_completed().await {
                Ok(tx) => println!("   ⛓️ Reportado en Solana: {}", tx),
                Err(e) => println!("   ⚠️ Solana (modo local): {}", e),
            }
        } else {
            println!("❌ Challenge {} falló", challenge.id);
        }

        counter += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

// 🌐 SERVIDOR WEB CON DASHBOARD
async fn start_web_server(node_state: Arc<RwLock<NodeState>>) {
    let state = Arc::clone(&node_state);

    let stats_route = warp::path("api")
        .and(warp::path("stats"))
        .and(warp::get())
        .and(with_state(state.clone()))
        .and_then(handle_stats);

    let dashboard_route = warp::path::end()
        .and(warp::get())
        .map(|| warp::reply::html(get_dashboard_html()));

    warp::serve(stats_route.or(dashboard_route))
        .run(([127, 0, 0, 1], 3000))
        .await;
}

fn with_state(state: Arc<RwLock<NodeState>>) -> impl Filter<Extract = (Arc<RwLock<NodeState>>,), Error = Infallible> + Clone {
    warp::any().map(move || Arc::clone(&state))
}

async fn handle_stats(state: Arc<RwLock<NodeState>>) -> Result<impl warp::Reply, Infallible> {
    let node = state.read().await;
    Ok(warp::reply::json(&json!({
        "pubkey": node.pubkey.to_string(),
        "cores": node.cores,
        "ram_gb": node.ram_gb,
        "reputation": node.reputation,
        "total_earned": node.total_earned,
        "tasks_completed": node.tasks_completed,
    })))
}

fn get_dashboard_html() -> &'static str {
    r#"
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>IaMine Dashboard</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: linear-gradient(135deg, #667eea, #764ba2); min-height: 100vh; padding: 20px; margin: 0; }
        .container { max-width: 800px; margin: 0 auto; }
        header { text-align: center; color: white; margin-bottom: 30px; }
        header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .stats-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 8px 16px rgba(0,0,0,0.1); }
        .label { color: #666; font-size: 0.9em; text-transform: uppercase; margin-bottom: 8px; }
        .value { font-size: 2em; font-weight: bold; color: #667eea; }
        .value.green { color: #10b981; }
        .info-row { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #eee; }
        .info-row:last-child { border-bottom: none; }
        .dot { display: inline-block; width: 12px; height: 12px; background: #10b981; border-radius: 50%; margin-right: 8px; }
        footer { text-align: center; color: white; margin-top: 30px; opacity: 0.8; }
    </style>
</head>
<body>
    <div class="container">
        <header><h1>🔥 IaMine</h1><p>La red eléctrica de la inteligencia artificial</p></header>
        <div class="stats-grid">
            <div class="card"><div class="label">💰 Ganado</div><div class="value green" id="earned">0 $MIND</div></div>
            <div class="card"><div class="label">⭐ Reputación</div><div class="value" id="reputation">100/100</div></div>
            <div class="card"><div class="label">✔️ Tareas</div><div class="value green" id="tasks">0</div></div>
            <div class="card"><div class="label">🔌 Estado</div><div class="value"><span class="dot"></span><span id="status">Activo</span></div></div>
        </div>
        <div class="card">
            <h2 style="margin-bottom:15px">ℹ️ Información del Nodo</h2>
            <div class="info-row"><span>Wallet:</span><span id="pubkey" style="word-break:break-all;margin-left:10px">...</span></div>
            <div class="info-row"><span>Cores:</span><span id="cores">-</span></div>
            <div class="info-row"><span>RAM:</span><span id="ram">-</span></div>
        </div>
        <footer><p>Actualizado cada 2s • IaMine Labs 2026</p></footer>
    </div>
    <script>
        async function fetchStats() {
            try {
                const d = await (await fetch('/api/stats')).json();
                document.getElementById('pubkey').textContent = d.pubkey;
                document.getElementById('cores').textContent = d.cores + ' cores';
                document.getElementById('ram').textContent = d.ram_gb + ' GB';
                document.getElementById('earned').textContent = d.total_earned + ' $MIND';
                document.getElementById('reputation').textContent = d.reputation + '/100';
                document.getElementById('tasks').textContent = d.tasks_completed;
            } catch(e) { console.error(e); }
        }
        fetchStats();
        setInterval(fetchStats, 2000);
    </script>
</body>
</html>
    "#
}
