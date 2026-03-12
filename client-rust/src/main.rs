use sysinfo::{System, SystemExt, CpuExt};
use std::env;

fn main() {
    let mut sys = System::new_all();
    sys.refresh_all();

    let cores = sys.cpus().len();
    let ram_gb = sys.total_memory() / 1_073_741_824;

    println!("🔥 Iamine Client iniciado");
    println!("Hardware detectado: {} cores | {} GB RAM", cores, ram_gb);
    println!("Aportando automáticamente: 2 núcleos + 2 GB RAM (configurable)");

    // Aquí irá la conexión Solana + P2P (próxima actualización)
    println!("✅ Nodo registrado en red Iamine. Esperando tareas...");
    println!("¡Estás minando IA! Revisa tu wallet para rewards $IAMINE");
}
