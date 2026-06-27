use crate::cli_flags::{parse_optional_string_flag, parse_optional_u32_flag};
use crate::node_modes::NodeMode;

pub(crate) fn lan_usage() -> String {
    "Uso: iamine-node lan [doctor [--json] [--network]|infer <prompt> --model <model_id> [--max-tokens N]]".to_string()
}

fn lan_infer_usage(reason: &str) -> String {
    format!(
        "{}\nUso: iamine-node lan infer <prompt> --model <model_id> [--max-tokens N]\nNext steps: iamine-node lan doctor; iamine-node models catalog; iamine-node models download <model_id>",
        reason
    )
}

pub(crate) fn parse_lan_infer_args(args: &[String]) -> Result<NodeMode, String> {
    let prompt = args
        .first()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| lan_infer_usage("Falta <prompt>"))?
        .clone();

    if args.iter().any(|arg| arg == "--prefer-local") {
        return Err(lan_infer_usage(
            "lan infer ejecuta inferencia LAN; elimina --prefer-local",
        ));
    }

    let model_id = parse_optional_string_flag(args, "--model")?
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| lan_infer_usage("Falta --model <model_id>"))?;
    let max_tokens_override = parse_optional_u32_flag(args, "--max-tokens")?;

    Ok(NodeMode::Infer {
        prompt,
        model_id: Some(model_id),
        max_tokens_override,
        force_network: true,
        no_local: false,
        prefer_local: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn lan_usage_lists_doctor_and_infer() {
        let usage = lan_usage();

        assert!(usage.contains("iamine-node lan"));
        assert!(usage.contains("doctor [--json] [--network]"));
        assert!(usage.contains("infer <prompt> --model <model_id>"));
    }

    #[test]
    fn lan_infer_maps_to_force_network_inference() {
        let mode = match parse_lan_infer_args(&args(&[
            "explica la red",
            "--model",
            "tinyllama-1b",
            "--max-tokens",
            "128",
        ])) {
            Ok(mode) => mode,
            Err(error) => {
                assert_eq!(error, "lan infer should parse");
                return;
            }
        };

        match mode {
            NodeMode::Infer {
                prompt,
                model_id,
                max_tokens_override,
                force_network,
                no_local,
                prefer_local,
            } => {
                assert_eq!(prompt, "explica la red");
                assert_eq!(model_id.as_deref(), Some("tinyllama-1b"));
                assert_eq!(max_tokens_override, Some(128));
                assert!(force_network);
                assert!(!no_local);
                assert!(!prefer_local);
            }
            other => {
                assert_eq!(format!("{other:?}"), "wrong NodeMode::Infer");
            }
        }
    }

    #[test]
    fn lan_infer_requires_explicit_model() {
        let error = match parse_lan_infer_args(&args(&["2+2"])) {
            Ok(mode) => {
                assert_eq!(format!("{mode:?}"), "missing model error not returned");
                return;
            }
            Err(error) => error,
        };

        assert!(error.contains("Falta --model <model_id>"));
        assert!(error.contains("iamine-node models catalog"));
    }

    #[test]
    fn lan_infer_rejects_local_preference() {
        let error = match parse_lan_infer_args(&args(&[
            "2+2",
            "--model",
            "tinyllama-1b",
            "--prefer-local",
        ])) {
            Ok(mode) => {
                assert_eq!(format!("{mode:?}"), "local preference error not returned");
                return;
            }
            Err(error) => error,
        };

        assert!(error.contains("elimina --prefer-local"));
    }
}
