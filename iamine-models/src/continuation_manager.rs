use crate::inference_engine::InferenceResult;

#[derive(Debug, Clone)]
pub struct ContinuationManager {
    pub max_steps: usize,
}

impl Default for ContinuationManager {
    fn default() -> Self {
        Self { max_steps: 3 }
    }
}

impl ContinuationManager {
    pub fn generate_with_continuation(
        &self,
        _prompt: &str,
        _model_id: &str,
        initial_result: InferenceResult,
    ) -> InferenceResult {
        initial_result
    }

    pub fn generate_with_runner<F>(
        &self,
        prompt: &str,
        model_id: &str,
        initial_result: InferenceResult,
        mut continue_step: F,
    ) -> InferenceResult
    where
        F: FnMut(String) -> InferenceResult,
    {
        let mut final_result = initial_result.clone();
        let mut final_output = initial_result.output.clone();
        let mut current_result = initial_result;
        let mut continuation_steps = 0usize;
        let mut total_tokens = final_result.tokens_generated;
        let mut total_ms = final_result.execution_ms;

        if current_result.truncated {
            println!("[Continuation] triggered");
        }

        while current_result.success
            && current_result.truncated
            && continuation_steps < self.max_steps
        {
            continuation_steps += 1;
            println!("[Continuation] step {}", continuation_steps);

            let continuation_prompt = Self::build_continuation_prompt(prompt, &final_output);
            let next_result = continue_step(continuation_prompt);

            if !next_result.success {
                final_result.success = false;
                final_result.error = next_result.error.clone();
                final_result.truncated = true;
                final_result.continuation_steps = continuation_steps;
                return final_result;
            }

            let cleaned = Self::remove_overlap(&final_output, &next_result.output);
            if !cleaned.trim().is_empty() {
                if !final_output.is_empty() && !final_output.ends_with('\n') {
                    final_output.push('\n');
                }
                final_output.push_str(cleaned.trim());
            }

            total_tokens += next_result.tokens_generated;
            total_ms += next_result.execution_ms;
            current_result = next_result;
        }

        println!("[Continuation] completed in {} steps", continuation_steps);

        final_result.output = final_output.trim().to_string();
        final_result.tokens_generated = total_tokens;
        final_result.execution_ms = total_ms;
        final_result.truncated = current_result.truncated;
        final_result.continuation_steps = continuation_steps;
        final_result.success = current_result.success;
        final_result.error = current_result.error.clone();
        final_result.model_id = model_id.to_string();
        final_result
    }

    pub fn build_continuation_prompt(original_prompt: &str, last_output: &str) -> String {
        let tail = Self::tail_chars(last_output, 400);
        format!(
            "{original_prompt}\n\nPartial answer:\n{tail}\n\nContinue from where you left off. Do not repeat."
        )
    }

    pub fn remove_overlap(prev: &str, next: &str) -> String {
        let prev_trimmed = prev.trim_end();
        let next_trimmed = next.trim_start();
        let max_overlap = prev_trimmed.len().min(next_trimmed.len()).min(240);

        for size in (1..=max_overlap).rev() {
            if prev_trimmed
                .chars()
                .rev()
                .take(size)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
                == next_trimmed.chars().take(size).collect::<String>()
            {
                return next_trimmed.chars().skip(size).collect::<String>();
            }
        }

        next_trimmed.to_string()
    }

    fn tail_chars(text: &str, count: usize) -> String {
        let chars: Vec<char> = text.chars().collect();
        let start = chars.len().saturating_sub(count);
        chars[start..].iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn success(output: &str, tokens: u32, truncated: bool) -> InferenceResult {
        InferenceResult {
            task_id: "task-1".to_string(),
            model_id: "llama3-3b".to_string(),
            output: output.to_string(),
            tokens_generated: tokens,
            truncated,
            continuation_steps: 0,
            execution_ms: 10,
            success: true,
            error: None,
            accelerator_used: "Metal".to_string(),
        }
    }

    #[test]
    fn test_continuation_trigger() {
        let manager = ContinuationManager::default();
        let initial = success("Primera parte", 100, true);
        let result = manager.generate_with_runner("prompt", "llama3-3b", initial, |_| {
            success("Segunda parte", 50, false)
        });

        assert_eq!(result.continuation_steps, 1);
        assert!(!result.truncated);
        assert!(result.output.contains("Segunda parte"));
    }

    #[test]
    fn test_multi_step_generation() {
        let manager = ContinuationManager { max_steps: 3 };
        let initial = success("Parte uno", 100, true);
        let mut step = 0usize;

        let result = manager.generate_with_runner("prompt", "llama3-3b", initial, |_| {
            step += 1;
            match step {
                1 => success("Parte dos", 80, true),
                _ => success("Parte tres", 60, false),
            }
        });

        assert_eq!(result.continuation_steps, 2);
        assert!(!result.truncated);
        assert!(result.output.contains("Parte uno"));
        assert!(result.output.contains("Parte dos"));
        assert!(result.output.contains("Parte tres"));
    }

    #[test]
    fn test_no_overlap() {
        let cleaned = ContinuationManager::remove_overlap(
            "La relatividad describe el espacio-tiempo",
            "espacio-tiempo y la gravedad en el universo.",
        );

        assert_eq!(cleaned, " y la gravedad en el universo.");
    }

    #[test]
    fn test_max_steps_limit() {
        let manager = ContinuationManager { max_steps: 1 };
        let initial = success("Parte uno", 100, true);

        let result = manager.generate_with_runner("prompt", "llama3-3b", initial, |_| {
            success("Parte dos", 80, true)
        });

        assert_eq!(result.continuation_steps, 1);
        assert!(result.truncated);
    }
}
