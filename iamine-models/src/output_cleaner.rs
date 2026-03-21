pub fn clean_output(text: String) -> String {
    let mut cleaned = text
        .replace("\r\n", "\n")
        .replace("<|assistant|>", "")
        .replace("<|user|>", "")
        .replace("<|system|>", "")
        .replace("</s>", "")
        .replace("<s>", "")
        .replace("[/INST]", "")
        .trim()
        .to_string();

    cleaned = collapse_repeated_lines(&cleaned);
    cleaned = collapse_repeated_sentences(&cleaned);
    cleaned = collapse_repeated_words(&cleaned);
    cleaned = trim_incomplete_tail(&cleaned);

    cleaned.trim().to_string()
}

fn collapse_repeated_lines(text: &str) -> String {
    let mut kept = Vec::new();
    let mut prev = "";
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed != prev {
            kept.push(trimmed);
            prev = trimmed;
        }
    }
    kept.join("\n")
}

fn collapse_repeated_sentences(text: &str) -> String {
    let mut result = Vec::new();
    let mut prev = String::new();
    for sentence in text.split_inclusive(['.', '!', '?', '\n']) {
        let trimmed = sentence.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed != prev {
            result.push(trimmed.to_string());
            prev = trimmed.to_string();
        }
    }
    if result.is_empty() {
        text.to_string()
    } else {
        result.join(" ")
    }
}

fn collapse_repeated_words(text: &str) -> String {
    let mut result = Vec::new();
    let mut prev = "";
    let mut repeated = 0usize;

    for word in text.split_whitespace() {
        if word == prev {
            repeated += 1;
            if repeated >= 2 {
                continue;
            }
        } else {
            repeated = 0;
            prev = word;
        }
        result.push(word);
    }

    result.join(" ")
}

fn trim_incomplete_tail(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    if trimmed.ends_with(['.', '!', '?', '"']) {
        return trimmed.to_string();
    }

    if let Some(idx) = trimmed.rfind(['.', '!', '?']) {
        let head = &trimmed[..=idx];
        if head.len() >= trimmed.len() / 2 {
            return head.trim().to_string();
        }
    }

    trimmed.to_string()
}
