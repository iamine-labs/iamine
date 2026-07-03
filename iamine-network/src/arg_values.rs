#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepeatedFlagArgError {
    MissingValue,
}

pub(crate) fn values_from_repeated_flag<'a>(
    args: &'a [String],
    flag: &'static str,
) -> Result<Vec<&'a str>, RepeatedFlagArgError> {
    let mut values = Vec::new();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];
        if arg == flag {
            let Some(value) = args.get(index + 1) else {
                return Err(RepeatedFlagArgError::MissingValue);
            };
            if value.starts_with("--") {
                return Err(RepeatedFlagArgError::MissingValue);
            }
            values.push(value.as_str());
            index += 2;
            continue;
        }

        if let Some(value) = arg
            .strip_prefix(flag)
            .and_then(|remaining| remaining.strip_prefix('='))
        {
            values.push(value);
        }
        index += 1;
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn repeated_flag_values_support_separate_and_equals_forms() {
        let raw_args = args(&["iamine-node", "--seed", "one", "--seed=two"]);
        let values = values_from_repeated_flag(&raw_args, "--seed").expect("values should parse");

        assert_eq!(values, vec!["one", "two"]);
    }

    #[test]
    fn repeated_flag_values_reject_missing_value() {
        let raw_args = args(&["iamine-node", "--seed"]);
        let result = values_from_repeated_flag(&raw_args, "--seed");

        assert!(matches!(result, Err(RepeatedFlagArgError::MissingValue)));
    }
}
