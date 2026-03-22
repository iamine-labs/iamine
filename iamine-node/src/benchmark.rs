use sha2::{Digest, Sha256};
use std::io::Write;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct NodeBenchmark {
    pub cpu_score: f64, // operaciones/segundo
    pub ram_available_gb: u64,
    pub disk_speed_mb: f64, // MB/s escritura
    pub gpu_available: bool,
    pub gpu_vram_gb: u64,
}

impl NodeBenchmark {
    /// Benchmark completo — máx 5 segundos
    pub fn run() -> Self {
        println!("🔬 Ejecutando benchmark del sistema...");

        let cpu_score = benchmark_cpu();
        let ram_available_gb = detect_ram();
        let disk_speed_mb = benchmark_disk();
        let (gpu_available, gpu_vram_gb) = detect_gpu();

        let b = NodeBenchmark {
            cpu_score,
            ram_available_gb,
            disk_speed_mb,
            gpu_available,
            gpu_vram_gb,
        };
        b.display();
        b
    }

    /// Calcula slots dinámicos según benchmark + policy
    pub fn calculate_slots(&self, policy: &crate::resource_policy::ResourcePolicy) -> usize {
        let base = (self.cpu_score / 2000.0) as usize;
        let slots = base.max(1).min(policy.cpu_cores);

        // Ajustar por RAM disponible (mínimo 1GB por slot)
        let ram_slots = self.ram_available_gb.max(1) as usize;
        let final_slots = slots.min(ram_slots);

        println!(
            "⚡ Worker slots calculados: {} (cpu_score={:.0}, ram={}GB, cores={})",
            final_slots, self.cpu_score, self.ram_available_gb, policy.cpu_cores
        );

        final_slots.max(1)
    }

    pub fn display(&self) {
        println!("📊 Benchmark completado:");
        println!("   CPU Score:    {:.0} ops/sec", self.cpu_score);
        println!("   RAM:          {} GB disponibles", self.ram_available_gb);
        println!("   Disk:         {:.1} MB/s", self.disk_speed_mb);
        println!(
            "   GPU:          {}",
            if self.gpu_available {
                format!("✅ {} GB VRAM", self.gpu_vram_gb)
            } else {
                "❌ No detectada".to_string()
            }
        );
    }
}

fn benchmark_cpu() -> f64 {
    print!("   CPU benchmark... ");
    let _ = std::io::stdout().flush();

    let start = Instant::now();
    let duration = std::time::Duration::from_secs(2);
    let mut ops = 0u64;
    let mut data = b"iamine_benchmark_data".to_vec();

    while start.elapsed() < duration {
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let result = hasher.finalize();
        data = result.to_vec();
        ops += 1;
    }

    let score = ops as f64 / 2.0; // ops per second
    println!("{:.0} ops/sec ✅", score);
    score
}

fn detect_ram() -> u64 {
    print!("   RAM detection... ");
    let _ = std::io::stdout().flush();

    // Leer /proc/meminfo en Linux o usar sysinfo
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemAvailable:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0);
                    let gb = kb / 1_048_576;
                    println!("{} GB ✅", gb);
                    return gb;
                }
            }
        }
    }

    // macOS o fallback
    let gb = 8u64; // default conservador
    println!("{} GB (estimado) ✅", gb);
    gb
}

fn benchmark_disk() -> f64 {
    print!("   Disk benchmark... ");
    let _ = std::io::stdout().flush();

    let path = "/tmp/iamine_bench_tmp";
    let data = vec![0u8; 10 * 1024 * 1024]; // 10 MB

    let start = Instant::now();
    if let Ok(mut f) = std::fs::File::create(path) {
        let _ = f.write_all(&data);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let _ = std::fs::remove_file(path);

    let speed = if elapsed > 0.0 { 10.0 / elapsed } else { 100.0 };
    println!("{:.1} MB/s ✅", speed);
    speed
}

fn detect_gpu() -> (bool, u64) {
    print!("   GPU detection... ");
    let _ = std::io::stdout().flush();

    // Intentar nvidia-smi
    if let Ok(output) = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=memory.total", "--format=csv,noheader,nounits"])
        .output()
    {
        if output.status.success() {
            let vram_mb: u64 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .unwrap_or(0);
            let vram_gb = vram_mb / 1024;
            println!("NVIDIA {} GB VRAM ✅", vram_gb);
            return (true, vram_gb);
        }
    }

    // Intentar Metal (macOS)
    #[cfg(target_os = "macos")]
    {
        if std::process::Command::new("system_profiler")
            .arg("SPDisplaysDataType")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            println!("Apple Metal ✅");
            return (true, 0);
        }
    }

    println!("No detectada");
    (false, 0)
}
