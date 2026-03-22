pub fn normalize_output(task_type: &str, exact_subtype: Option<&str>, output: &str) -> (String, Option<String>) {
    match task_type {
        "ExactMath" => normalize_exact_math(exact_subtype, output),
        "StructuredList" => normalize_structured_list(output),
        "Deterministic" => normalize_deterministic(output),
        _ => (output.trim().to_string(), None),
    }
}

fn normalize_exact_math(exact_subtype: Option<&str>, output: &str) -> (String, Option<String>) {
    let trimmed = output.trim();
    match exact_subtype {
        Some("Integer") => {
            let normalized = extract_last_integer(trimmed).unwrap_or_else(|| {
                trimmed
                    .trim_end_matches(|c: char| matches!(c, '.' | ',' | ';' | ':'))
                    .trim()
                    .to_string()
            });
            let reason = if normalized != trimmed {
                Some("Integer cleanup".to_string())
            } else {
                None
            };
            (normalized, reason)
        }
        Some("DecimalSequence") => {
            let normalized = extract_decimal_sequence(trimmed).unwrap_or_else(|| {
                trimmed
                    .chars()
                    .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                    .collect::<String>()
            });
            let reason = if normalized != trimmed {
                Some("DecimalSequence fix".to_string())
            } else {
                None
            };
            (normalized, reason)
        }
        _ => normalize_deterministic(output),
    }
}

fn normalize_structured_list(output: &str) -> (String, Option<String>) {
    if let Some(alphabet) = normalize_alphabet_sequence(output) {
        let reason = if alphabet != output.trim() {
            Some("StructuredList alphabet fix".to_string())
        } else {
            None
        };
        return (alphabet, reason);
    }

    let compact = output.replace('\n', " ");
    let normalized = compact
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" ,", ",")
        .replace(", ", ", ")
        .trim()
        .to_string();

    let reason = if normalized != output.trim() {
        Some("StructuredList spacing fix".to_string())
    } else {
        None
    };

    (normalized, reason)
}

fn normalize_deterministic(output: &str) -> (String, Option<String>) {
    let normalized = output.split_whitespace().collect::<Vec<_>>().join(" ");
    let reason = if normalized != output.trim() {
        Some("Deterministic spacing fix".to_string())
    } else {
        None
    };
    (normalized.trim().to_string(), reason)
}

fn normalize_alphabet_sequence(output: &str) -> Option<String> {
    let letters: Vec<char> = output
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|part| part.len() == 1)
        .filter_map(|part| part.chars().next())
        .filter(|c| c.is_ascii_uppercase())
        .collect();

    let alphabet: Vec<char> = ('A'..='Z').collect();
    if letters == alphabet {
        Some(
            alphabet
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    } else {
        None
    }
}

fn extract_last_integer(output: &str) -> Option<String> {
    let chars: Vec<char> = output.chars().collect();
    let mut i = chars.len();

    while i > 0 {
        i -= 1;
        if !chars[i].is_ascii_digit() {
            continue;
        }

        let end = i + 1;
        let mut start = i;
        while start > 0 && chars[start - 1].is_ascii_digit() {
            start -= 1;
        }
        if start > 0 && matches!(chars[start - 1], '-' | '+') {
            start -= 1;
        }

        let candidate: String = chars[start..end].iter().collect();
        if candidate.chars().any(|c| c.is_ascii_digit()) {
            return Some(candidate);
        }
    }

    None
}

fn extract_decimal_sequence(output: &str) -> Option<String> {
    let chars: Vec<char> = output.chars().collect();
    let mut i = 0usize;

    while i < chars.len() {
        if !chars[i].is_ascii_digit() {
            i += 1;
            continue;
        }

        let mut candidate = String::new();
        let mut j = i;
        let mut saw_dot = false;
        let mut saw_fraction_digit = false;

        while j < chars.len() {
            let ch = chars[j];
            if ch.is_ascii_digit() {
                candidate.push(ch);
                if saw_dot {
                    saw_fraction_digit = true;
                }
                j += 1;
                continue;
            }

            if ch == '.' && !saw_dot {
                saw_dot = true;
                candidate.push(ch);
                j += 1;
                while j < chars.len() && chars[j].is_ascii_whitespace() {
                    j += 1;
                }
                continue;
            }

            break;
        }

        if saw_dot && saw_fraction_digit {
            return Some(candidate);
        }

        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::normalize_output;

    #[test]
    fn test_integer_normalization() {
        let (normalized, reason) = normalize_output("ExactMath", Some("Integer"), "54.");
        assert_eq!(normalized, "54");
        assert_eq!(reason.as_deref(), Some("Integer cleanup"));
    }

    #[test]
    fn test_integer_normalization_extracts_final_answer() {
        let (normalized, reason) = normalize_output("ExactMath", Some("Integer"), "2 + 2 = 4.");
        assert_eq!(normalized, "4");
        assert_eq!(reason.as_deref(), Some("Integer cleanup"));
    }

    #[test]
    fn test_decimal_normalization() {
        let (normalized, reason) = normalize_output("ExactMath", Some("DecimalSequence"), "3. 14159");
        assert_eq!(normalized, "3.14159");
        assert_eq!(reason.as_deref(), Some("DecimalSequence fix"));
    }

    #[test]
    fn test_decimal_normalization_removes_newline_after_decimal() {
        let (normalized, reason) = normalize_output("ExactMath", Some("DecimalSequence"), "Pi = 3.\n14159");
        assert_eq!(normalized, "3.14159");
        assert_eq!(reason.as_deref(), Some("DecimalSequence fix"));
    }

    #[test]
    fn test_pi_format_fix() {
        let (normalized, _) = normalize_output(
            "ExactMath",
            Some("DecimalSequence"),
            "3. 1415926535",
        );
        assert_eq!(normalized, "3.1415926535");
    }

    #[test]
    fn test_structured_list_normalization() {
        let (normalized, _) = normalize_output("StructuredList", None, "A,  B,   C");
        assert_eq!(normalized, "A, B, C");
    }

    #[test]
    fn test_alphabet_sequence_normalization() {
        let (normalized, reason) = normalize_output(
            "StructuredList",
            None,
            "Las letras son: A, B, C, D, E, F, G, H, I, J, K, L, M, N, , O, P, Q, R, S, T, U, V, W, X, Y, Z",
        );
        assert_eq!(
            normalized,
            "A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z"
        );
        assert_eq!(reason.as_deref(), Some("StructuredList alphabet fix"));
    }
}
