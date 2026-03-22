#[derive(Debug, Clone)]
pub struct ResourcePolicy {
    pub cpu_cores: usize,
    pub max_cpu_load: u8, // 0-100 %
    pub ram_limit_gb: u64,
    pub gpu_enabled: bool,
    pub disk_limit_gb: u64,
    pub disk_path: String,
}

impl ResourcePolicy {
    /// Parsea desde args de CLI
    pub fn from_args(args: &[String]) -> Self {
        let cpu_cores = parse_arg(args, "--cpu").unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2) as u64
        }) as usize;

        let max_cpu_load = parse_arg(args, "--max-load").unwrap_or(80) as u8;
        let ram_limit_gb = parse_arg(args, "--ram").unwrap_or(4);
        let gpu_enabled = args.iter().any(|a| a == "--gpu");
        let disk_limit_gb = parse_arg(args, "--disk-limit").unwrap_or(10);
        let disk_path = args
            .iter()
            .find(|a| a.starts_with("--disk-path="))
            .map(|a| a.replace("--disk-path=", ""))
            .unwrap_or_else(|| "/tmp/iamine".to_string());

        ResourcePolicy {
            cpu_cores,
            max_cpu_load,
            ram_limit_gb,
            gpu_enabled,
            disk_limit_gb,
            disk_path,
        }
    }

    pub fn display(&self) {
        println!("⚙️  Resource Policy:");
        println!("   CPU cores:    {}", self.cpu_cores);
        println!("   Max CPU load: {}%", self.max_cpu_load);
        println!("   RAM limit:    {} GB", self.ram_limit_gb);
        println!(
            "   GPU:          {}",
            if self.gpu_enabled { "✅" } else { "❌" }
        );
        println!(
            "   Disk limit:   {} GB @ {}",
            self.disk_limit_gb, self.disk_path
        );
    }
}

fn parse_arg(args: &[String], flag: &str) -> Option<u64> {
    // --flag=N o --flag N
    for (i, arg) in args.iter().enumerate() {
        if arg.starts_with(&format!("{}=", flag)) {
            return arg.split('=').nth(1)?.parse().ok();
        }
        if arg == flag {
            return args.get(i + 1)?.parse().ok();
        }
    }
    None
}
