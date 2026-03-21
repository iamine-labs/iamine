use crate::task_analyzer::{detect_task_type, TaskType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Language {
    English,
    Spanish,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Complexity {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptProfile {
    pub language: Language,
    pub complexity: Complexity,
    pub length: usize,
    pub task_type: TaskType,
}

pub fn analyze_prompt(prompt: &str) -> PromptProfile {
    let trimmed = prompt.trim();
    let lower = format!(" {} ", trimmed.to_lowercase());
    let words = trimmed.split_whitespace().count();
    let chars = trimmed.chars().count();

    let spanish_hits = [
        " que ",
        " explica",
        " explicame",
        " dame",
        " lista",
        " listalos",
        " enumera",
        " escribe",
        " primeros",
        " digitos",
        " dígitos",
        " por que",
        " como ",
        " agujero",
        " negro",
        " relatividad",
        " teoria",
        " español",
        " espanol",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let english_hits = [
        " what ",
        " explain",
        " list",
        " write",
        " first ",
        " digits",
        " why ",
        " how ",
        " gravity",
        " theory",
        " black hole",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let language = if spanish_hits > english_hits && spanish_hits > 0 {
        Language::Spanish
    } else if english_hits > spanish_hits && english_hits > 0 {
        Language::English
    } else {
        Language::Unknown
    };

    let conceptual_hits = [
        " explain",
        " theory",
        " why ",
        " how ",
        " relatividad",
        " agujero negro",
        " black hole",
        " compare",
        " difference",
        " concept",
        " porque",
        " por que",
        " como funciona",
    ]
    .iter()
    .filter(|needle| lower.contains(**needle))
    .count();

    let complexity = if chars >= 80 || words >= 14 || conceptual_hits >= 2 {
        Complexity::High
    } else if chars >= 25 || words >= 5 || conceptual_hits >= 1 {
        Complexity::Medium
    } else {
        Complexity::Low
    };

    PromptProfile {
        language,
        complexity,
        length: chars,
        task_type: detect_task_type(trimmed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_detection_spanish() {
        let profile = analyze_prompt("explica la teoria de la relatividad");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.task_type, TaskType::Conceptual);
    }

    #[test]
    fn test_pi_digits_classification() {
        let profile = analyze_prompt("dame los primeros 100 digitos de pi");
        assert_eq!(profile.language, Language::Spanish);
        assert_eq!(profile.task_type, TaskType::ExactMath);
    }

    #[test]
    fn test_complexity_detection() {
        let low = analyze_prompt("What is 2+2?");
        let high = analyze_prompt("Explain the theory of relativity and why space-time bends around massive objects.");

        assert_eq!(low.complexity, Complexity::Low);
        assert_eq!(high.complexity, Complexity::High);
    }
}
