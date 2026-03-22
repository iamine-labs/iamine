pub fn validate_structured_output(task_type: &str, output: &str) -> bool {
    validate_structure(task_type, output) && validate_exactness(task_type, None, output)
}

pub fn validate_structure(task_type: &str, output: &str) -> bool {
    match task_type {
        "StructuredList" => validate_structured_list(output),
        "ExactMath" | "Deterministic" => validate_deterministic(output),
        _ => true,
    }
}

pub fn validate_exactness(task_type: &str, exact_subtype: Option<&str>, output: &str) -> bool {
    match task_type {
        "ExactMath" => validate_exact_math(exact_subtype, output),
        "Deterministic" => validate_deterministic(output),
        _ => true,
    }
}

fn validate_structured_list(output: &str) -> bool {
    let compact = output.replace('\n', " ");
    if compact.contains(", ,") {
        return false;
    }
    let numbered_items = extract_numbered_items(&compact);
    if !numbered_items.is_empty() {
        if numbered_items.iter().any(|item| item.trim().is_empty()) {
            return false;
        }

        if compact.to_lowercase().contains("abecedario") || compact.contains("1. A") {
            let expected: Vec<String> = ('A'..='Z').map(|c| c.to_string()).collect();
            return numbered_items == expected;
        }
    }

    if compact.to_lowercase().contains("abecedario") || compact.contains("A,") {
        return alphabet_sequence_is_complete(&compact);
    }

    !compact.trim().is_empty()
}

fn validate_exact_math(exact_subtype: Option<&str>, output: &str) -> bool {
    let trimmed = output.trim();
    let has_digits = trimmed.chars().any(|c| c.is_ascii_digit());
    let refusal = contains_any(
        &trimmed.to_lowercase(),
        &[
            "lo siento",
            "i can't",
            "cannot",
            "no puedo",
            "no puedo proporcionar",
        ],
    );

    if !has_digits || refusal {
        return false;
    }

    match exact_subtype {
        Some("Integer") => trimmed
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_whitespace() || matches!(c, '-' | '+')),
        Some("DecimalSequence") => {
            let decimal_points = trimmed.chars().filter(|c| *c == '.').count();
            decimal_points <= 1
                && !trimmed.contains(". ")
                && trimmed.chars().all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+'))
        }
        _ => true,
    }
}

fn validate_deterministic(output: &str) -> bool {
    let lower = output.to_lowercase();
    !contains_any(
        &lower,
        &[
            "lo siento",
            "i can't",
            "cannot",
            "no puedo",
            "¿te gustaría?",
            "would you like",
        ],
    )
}

fn alphabet_sequence_is_complete(output: &str) -> bool {
    let letters: Vec<char> = output
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|part| part.len() == 1)
        .filter_map(|part| part.chars().next())
        .filter(|c| c.is_ascii_uppercase())
        .collect();

    let alphabet: Vec<char> = ('A'..='Z').collect();
    letters == alphabet
}

fn extract_numbered_items(output: &str) -> Vec<String> {
    let bytes = output.as_bytes();
    let mut starts = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i].is_ascii_digit() && (i == 0 || bytes[i - 1].is_ascii_whitespace()) {
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'.' {
                starts.push((i, j + 1));
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }

    let mut items = Vec::new();
    for window in starts.windows(2) {
        let (_, current_start) = window[0];
        let (next_idx, _) = window[1];
        items.push(output[current_start..next_idx].trim().to_string());
    }

    if let Some((_, current_start)) = starts.last().copied() {
        items.push(output[current_start..].trim().to_string());
    }

    items
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{validate_exactness, validate_structured_output};

    #[test]
    fn test_abecedario_validation() {
        let valid = "A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z";
        let invalid = "A, B, C, D, E, F, G, H, I, J, K, L, M, N, , O, P, Q, R, S, T, U, V, W, X, Y, Z";

        assert!(validate_structured_output("StructuredList", valid));
        assert!(!validate_structured_output("StructuredList", invalid));
    }

    #[test]
    fn test_decimal_exactness() {
        assert!(!validate_exactness("ExactMath", Some("DecimalSequence"), "3. 14159"));
        assert!(validate_exactness("ExactMath", Some("DecimalSequence"), "3.14159"));
    }
}
