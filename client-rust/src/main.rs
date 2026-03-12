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

#[tokio::main]
async fn main() {
    println!("🔥 MindMine Client iniciado");
    
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
    let sm = Arc::clone(&solana_manager);
    match sm.register_node(cores, ram_gb).await {
        Ok(tx_sig) => println!("   Tx: {}\n", tx_sig),
        Err(e) => {
            println!("   ❌ Error: {}\n", e);
            return;
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
    println!("🌐 Iniciando servidor web en http://localhost:3000");
    let web_server_state = Arc::clone(&node_state);
    let web_server = task::spawn(async move {
        start_web_server(web_server_state).await;
    });
    
    // 7️⃣ CONECTAR A RED P2P
    println!("🌐 Conectando a red P2P MindMine...");
    
    let task_queue = Arc::new(RwLock::new(Vec::new()));
    let task_queue_clone = Arc::clone(&task_queue);
    let node_state_clone = Arc::clone(&node_state);
    let solana_manager_clone = Arc::clone(&solana_manager);
    
    // Tarea 1: Escuchar challenges
    let listener_task = task::spawn(async move {
        listen_for_challenges(task_queue_clone).await;
    });
    
    // Tarea 2: Procesar challenges (con Solana)
    let processor_task = task::spawn(async move {
        process_challenges(task_queue, node_state_clone, solana_manager_clone).await;
    });
    
    println!("✅ MindMine Client activo. Esperando tareas...");
    println!("💰 Abre http://localhost:3000 para ver tus ganancias\n");
    
    let _ = tokio::join!(listener_task, processor_task, web_server);
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
    
    let routes = stats_route.or(dashboard_route);
    
    warp::serve(routes)
        .run(([127, 0, 0, 1], 3000))
        .await;
}

fn with_state(
    state: Arc<RwLock<NodeState>>,
) -> impl Filter<Extract = (Arc<RwLock<NodeState>>,), Error = Infallible> + Clone {
    warp::any().map(move || Arc::clone(&state))
}

async fn handle_stats(
    state: Arc<RwLock<NodeState>>,
) -> Result<impl warp::Reply, Infallible> {
    let node = state.read().await;
    let response = json!({
        "pubkey": node.pubkey.to_string(),
        "cores": node.cores,
        "ram_gb": node.ram_gb,
        "reputation": node.reputation,
        "total_earned": node.total_earned,
        "tasks_completed": node.tasks_completed,
    });
    Ok(warp::reply::json(&response))
}

fn get_dashboard_html() -> &'static str {
    r#"
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MindMine Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 800px;
            margin: 0 auto;
        }
        
        header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        header p {
            font-size: 1.1em;
            opacity: 0.9;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 8px;
        }
        
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        
        .stat-value.success {
            color: #10b981;
        }
        
        .info-card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        
        .info-card h2 {
            color: #333;
            margin-bottom: 15px;
            font-size: 1.3em;
        }
        
        .info-row {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        
        .info-row:last-child {
            border-bottom: none;
        }
        
        .info-label {
            color: #666;
        }
        
        .info-value {
            color: #333;
            font-weight: 500;
            word-break: break-all;
            text-align: right;
            flex: 1;
            margin-left: 20px;
        }
        
        .status-online {
            display: inline-block;
            width: 12px;
            height: 12px;
            background: #10b981;
            border-radius: 50%;
            margin-right: 8px;
        }
        
        footer {
            text-align: center;
            color: white;
            margin-top: 30px;
            opacity: 0.8;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔥 IaMine</h1>
            <p>La red eléctrica de la inteligencia artificial</p>
        </header>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">💰 Ganado</div>
                <div class="stat-value success" id="earned">0 $MIND</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">⭐ Reputación</div>
                <div class="stat-value" id="reputation">100/100</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">✔️ Tareas</div>
                <div class="stat-value success" id="tasks">0</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">🔌 Estado</div>
                <div class="stat-value">
                    <span class="status-online"></span>
                    <span id="status">Activo</span>
                </div>
            </div>
        </div>
        
        <div class="info-card">
            <h2>ℹ️ Información del Nodo</h2>
            <div class="info-row">
                <span class="info-label">Wallet:</span>
                <span class="info-value" id="pubkey">Cargando...</span>
            </div>
            <div class="info-row">
                <span class="info-label">Cores:</span>
                <span class="info-value" id="cores">-</span>
            </div>
            <div class="info-row">
                <span class="info-label">RAM:</span>
                <span class="info-value" id="ram">-</span>
            </div>
        </div>
        
        <footer>
            <p>Actualizado en tiempo real • MindMine Labs 2026</p>
        </footer>
    </div>
    
    <script>
        async function fetchStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                document.getElementById('pubkey').textContent = data.pubkey;
                document.getElementById('cores').textContent = data.cores + ' cores';
                document.getElementById('ram').textContent = data.ram_gb + ' GB';
                document.getElementById('earned').textContent = data.total_earned + ' $MIND';
                document.getElementById('reputation').textContent = data.reputation + '/100';
                document.getElementById('tasks').textContent = data.tasks_completed;
                document.getElementById('status').textContent = 'Activo';
            } catch (error) {
                console.error('Error fetching stats:', error);
            }
        }
        
        fetchStats();
        setInterval(fetchStats, 2000);
    </script>
</body>
</html>
    "#
}

// 🔵 ESCUCHAR CHALLENGES
async fn listen_for_challenges(task_queue: Arc<RwLock<Vec<Challenge>>>) {
    let mut counter = 1;
    loop {
        let challenge = Challenge {
            id: format!("challenge_{:03}", counter),
            data: format!("task_data_{}", counter).into_bytes(),
            expected_hash: compute_sha256(format!("task_data_{}", counter).as_bytes()),
            reward: 50 + (counter as u64 * 10),
            node_assignment: None,
        };
        
        println!("📥 Challenge recibido: {}", challenge.id);
        
        let mut queue = task_queue.write().await;
        queue.push(challenge);
        
        counter += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }
}

// 🟢 PROCESAR CHALLENGES (con Solana)
async fn process_challenges(
    task_queue: Arc<RwLock<Vec<Challenge>>>,
    node_state: Arc<RwLock<NodeState>>,
    solana_manager: Arc<SolanaManager>,
) {
    loop {
        let mut queue = task_queue.write().await;
        
        if let Some(challenge) = queue.pop() {
            drop(queue);
            
            let result_hash = compute_sha256(&challenge.data);
            let is_correct = result_hash == challenge.expected_hash;
            
            if is_correct {
                // Enviar a Solana
                let sm = Arc::clone(&solana_manager);
                let _ = sm.submit_challenge_response(&challenge.id, &result_hash).await;
                
                // Actualizar estado local
                let mut state = node_state.write().await;
                state.total_earned += challenge.reward;
                state.tasks_completed += 1;
                state.reputation = std::cmp::min(100, state.reputation + 5);
                println!("✅ Challenge {} completado: +{} $MIND", challenge.id, challenge.reward);
            } else {
                let mut state = node_state.write().await;
                state.reputation = state.reputation.saturating_sub(10);
                println!("❌ Challenge {} incorrecto", challenge.id);
            }
        }
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    format!("{:x}", result)
}

#[derive(Clone, Debug)]
struct Challenge {
    id: String,
    data: Vec<u8>,
    expected_hash: String,
    reward: u64,
    node_assignment: Option<Pubkey>,
}

#[derive(Clone, Debug)]
struct NodeState {
    pubkey: Pubkey,
    cores: u64,
    ram_gb: u64,
    reputation: u32,
    total_earned: u64,
    tasks_completed: u64,
}
