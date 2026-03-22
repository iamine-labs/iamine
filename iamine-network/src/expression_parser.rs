pub fn normalize_expression(prompt: &str) -> Option<String> {
    let trimmed = prompt.trim();
    if !looks_like_math_expression(trimmed) {
        return None;
    }

    let mut normalized = trimmed.replace('×', "*").replace('÷', "/");
    normalized.retain(|c| !c.is_ascii_whitespace());

    if let Some(square_root_phrase) = normalize_square_root(&normalized) {
        return Some(square_root_phrase);
    }

    while has_wrapping_parentheses(&normalized) {
        normalized = normalized[1..normalized.len() - 1].to_string();
    }

    normalized = insert_implicit_multiplication(&normalized);
    normalized = collapse_parenthesized_numbers(&normalized);

    if normalized != trimmed {
        Some(normalized)
    } else {
        None
    }
}

fn looks_like_math_expression(prompt: &str) -> bool {
    let trimmed = prompt.trim();
    if trimmed.is_empty() || !trimmed.chars().any(|c| c.is_ascii_digit()) {
        return false;
    }

    let compact: String = trimmed
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    if compact.is_empty() {
        return false;
    }

    let lower = compact.to_lowercase();
    let mut i = 0usize;
    let chars: Vec<char> = lower.chars().collect();
    while i < chars.len() {
        if chars[i].is_ascii_digit()
            || matches!(chars[i], '+' | '-' | '*' | '/' | '^' | '(' | ')' | '.')
        {
            i += 1;
            continue;
        }

        if lower[i..].starts_with("sqrt") {
            i += 4;
            continue;
        }

        return false;
    }

    compact
        .chars()
        .any(|c| matches!(c, '+' | '-' | '*' | '/' | '^' | '(' | ')'))
        || lower.starts_with("sqrt(")
}

fn has_wrapping_parentheses(expr: &str) -> bool {
    if expr.len() < 2 || !expr.starts_with('(') || !expr.ends_with(')') {
        return false;
    }

    let mut depth = 0i32;
    for (idx, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx != expr.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }

    depth == 0
}

fn insert_implicit_multiplication(expr: &str) -> String {
    let chars: Vec<char> = expr.chars().collect();
    let mut normalized = String::with_capacity(expr.len() + 4);

    for (idx, ch) in chars.iter().copied().enumerate() {
        if ch == '(' && idx > 0 && (chars[idx - 1].is_ascii_digit() || chars[idx - 1] == ')') {
            normalized.push('*');
        }
        normalized.push(ch);
    }

    normalized
}

fn collapse_parenthesized_numbers(expr: &str) -> String {
    let chars: Vec<char> = expr.chars().collect();
    let mut normalized = String::with_capacity(expr.len());
    let mut i = 0usize;

    while i < chars.len() {
        if chars[i] == '(' {
            let preceded_by_alpha = i > 0 && chars[i - 1].is_ascii_alphabetic();
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            if !preceded_by_alpha && j > i + 1 && j < chars.len() && chars[j] == ')' {
                for digit in &chars[i + 1..j] {
                    normalized.push(*digit);
                }
                i = j + 1;
                continue;
            }
        }

        normalized.push(chars[i]);
        i += 1;
    }

    normalized
}

fn normalize_square_root(expr: &str) -> Option<String> {
    let lower = expr.to_lowercase();
    if !lower.starts_with("sqrt(") || !lower.ends_with(')') {
        return None;
    }

    let inner = &expr[5..expr.len() - 1];
    if inner.is_empty() || !inner.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    Some(format!("square root of {}", inner))
}

#[cfg(test)]
mod tests {
    use super::normalize_expression;

    #[test]
    fn test_expression_normalization() {
        assert_eq!(normalize_expression("6(9)").as_deref(), Some("6*9"));
    }

    #[test]
    fn test_parentheses_math() {
        assert_eq!(normalize_expression("(2+2)").as_deref(), Some("2+2"));
    }

    #[test]
    fn test_power_expression() {
        assert_eq!(normalize_expression("2^10"), None);
    }

    #[test]
    fn test_sqrt_expression_is_normalized_semantically() {
        assert_eq!(
            normalize_expression("sqrt(16)").as_deref(),
            Some("square root of 16")
        );
    }
}
