pub(crate) fn usage_text() -> &'static str {
    "Uso:\n  iamine-node --worker [--port=N] [--cpu=N] [--ram=N] [--gpu]\n  iamine-node --relay\n  iamine-node --broadcast <type> <data>\n  iamine-node models list\n  iamine-node models stats\n  iamine-node models download <model_id>\n  iamine-node models remove <model_id>\n  iamine-node semantic-eval\n  iamine-node regression-run\n  iamine-node check-code\n  iamine-node check-security\n  iamine-node validate-release\n  iamine-node tasks stats [--json]\n  iamine-node tasks trace <task_id> [--json]\n  iamine-node cluster status [--json]\n  iamine-node cluster stress [--requests N] [--concurrency N] [--task TYPE] [--prefix TEXT] [--timeout-secs N] [--output-dir PATH] [--stop-on-first-failure] [--json]\n  iamine-node --daemon\nFlags:\n  --debug-network\n  --debug-scheduler\n  --debug-tasks\n  --force-network\n  --no-local\n  --prefer-local"
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
        assert!(usage.contains("iamine-node tasks stats [--json]"));
        assert!(usage.contains("iamine-node tasks trace <task_id> [--json]"));
        assert!(usage.contains("iamine-node cluster status [--json]"));
        assert!(usage.contains("iamine-node cluster stress [--requests N]"));
        assert!(usage.contains("--debug-network"));
        assert!(usage.contains("--force-network"));
    }
}
