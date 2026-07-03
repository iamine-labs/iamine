pub(crate) fn usage_text() -> &'static str {
    "Uso:\n  iamine-node --worker [--port=N] [--cpu=N] [--ram=N] [--gpu] [--bootnode ADDR]\n  iamine-node worker lifecycle [install|start|stop|restart|readiness|recover|status] [--port=N] [--json]\n  iamine-node node config [status|migrate|rollback] [--path PATH] [--yes] [--json]\n  iamine-node node identity [status|init] [--path PATH] [--json]\n  iamine-node --relay [--bootnode ADDR]\n  iamine-node --broadcast <type> <data> [--required-model MODEL] [--bootnode ADDR]\n  iamine-node models catalog\n  iamine-node models list\n  iamine-node models select\n  iamine-node models stats\n  iamine-node models download <model_id>\n  iamine-node models license accept <model_id> --yes\n  iamine-node models remove <model_id>\n  iamine-node semantic-eval\n  iamine-node regression-run\n  iamine-node check-code\n  iamine-node check-security\n  iamine-node validate-release\n  iamine-node tasks stats [--json]\n  iamine-node tasks trace <task_id> [--json]\n  iamine-node cluster status [--json]\n  iamine-node cluster stress [--requests N] [--concurrency N] [--task TYPE] [--required-model MODEL] [--batch-file PATH] [--prefix TEXT] [--timeout-secs N] [--output-dir PATH] [--stop-on-first-failure] [--json]\n  iamine-node lan doctor [--json] [--network]\n  iamine-node lan infer <prompt> --model <model_id> [--max-tokens N]\n  iamine-node hardware inspect [--json] [--dynamic]\n  iamine-node hardware show [--json]\n  iamine-node hardware refresh [--yes] [--json] [--dynamic]\n  iamine-node --daemon [--bootnode ADDR]\nFlags:\n  --bootnode ADDR or --bootnode=ADDR\n  --debug-network\n  --debug-scheduler\n  --debug-tasks\n  --force-network\n  --no-local\n  --prefer-local"
}

pub(crate) fn print_usage() {
    eprintln!("{}", usage_text());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_preserves_existing_help_text() {
        let usage = usage_text();

        assert!(usage.contains("iamine-node --broadcast <type> <data>"));
        assert!(usage.contains("iamine-node --worker [--port=N]"));
        assert!(usage.contains("--bootnode ADDR or --bootnode=ADDR"));
        assert!(usage.contains("iamine-node worker lifecycle"));
        assert!(usage.contains("iamine-node node config [status|migrate|rollback]"));
        assert!(usage.contains("iamine-node node identity [status|init]"));
        assert!(usage.contains("iamine-node models catalog"));
        assert!(usage.contains("iamine-node models select"));
        assert!(usage.contains("iamine-node models license accept <model_id> --yes"));
        assert!(usage.contains("iamine-node tasks stats [--json]"));
        assert!(usage.contains("iamine-node tasks trace <task_id> [--json]"));
        assert!(usage.contains("iamine-node cluster status [--json]"));
        assert!(usage.contains("iamine-node cluster stress [--requests N]"));
        assert!(usage.contains("iamine-node lan doctor [--json] [--network]"));
        assert!(usage.contains("iamine-node lan infer <prompt> --model <model_id>"));
        assert!(usage.contains("iamine-node hardware inspect [--json] [--dynamic]"));
        assert!(usage.contains("--debug-network"));
        assert!(usage.contains("--force-network"));
    }
}
