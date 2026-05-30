use crate::expression_parser::normalize_expression;
use crate::task_analyzer::TaskType;

pub(crate) fn determine_primary_task(
    trimmed: &str,
    lower: &str,
    detected_task_type: TaskType,
    requires_context: bool,
) -> TaskType {
    if requires_context {
        if contains_any(
            lower,
            &[
                "hazlo mas simple",
                "hazlo más simple",
                "simplifica",
                "explain it simply",
            ],
        ) {
            return TaskType::Conceptual;
        }
        return TaskType::General;
    }

    if is_hybrid_example_prompt(lower) {
        return TaskType::Conceptual;
    }

    if contains_any(lower, &["explica", "explain", "que es", "qué es"])
        && (has_exact_math_markers(trimmed, lower)
            || has_math_domain_markers(trimmed, lower)
            || matches!(
                detected_task_type,
                TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
            ))
    {
        return TaskType::Conceptual;
    }

    if is_scientific_reasoning_prompt(lower) {
        return TaskType::Reasoning;
    }

    if is_ideation_prompt(lower) {
        return TaskType::Generative;
    }

    if is_philosophical_prompt(lower) {
        return TaskType::Conceptual;
    }

    if is_conceptual_math_prompt(trimmed, lower) {
        return TaskType::Conceptual;
    }

    detected_task_type
}

pub(crate) fn detect_secondary_tasks(
    trimmed: &str,
    lower: &str,
    primary_task: TaskType,
    requires_context: bool,
) -> Vec<TaskType> {
    let mut tasks = Vec::new();

    if requires_context {
        return tasks;
    }

    if contains_any(
        lower,
        &[
            "explica", "explain", "que es", "qué es", "como", "how", "por que", "why",
        ],
    ) && primary_task != TaskType::Conceptual
    {
        push_task(&mut tasks, TaskType::Conceptual);
    }

    if is_ideation_prompt(lower)
        || contains_any(lower, &["ejemplo", "example", "ejercicio", "exercise"])
    {
        push_task(&mut tasks, TaskType::Generative);
    }

    if is_structured_output_request(trimmed, lower) {
        push_task(&mut tasks, TaskType::StructuredList);
    }

    let symbolic_markers = has_symbolic_math_markers(trimmed, lower);
    if symbolic_markers && primary_task != TaskType::SymbolicMath {
        push_task(&mut tasks, TaskType::SymbolicMath);
    } else if has_math_domain_markers(trimmed, lower)
        && !matches!(
            primary_task,
            TaskType::Math | TaskType::ExactMath | TaskType::SymbolicMath
        )
    {
        push_task(&mut tasks, TaskType::Math);
    }

    if has_exact_math_markers(trimmed, lower)
        && !symbolic_markers
        && primary_task != TaskType::ExactMath
    {
        push_task(&mut tasks, TaskType::ExactMath);
    }

    if contains_any(
        lower,
        &[
            "step by step",
            "paso a paso",
            "que pasaria",
            "qué pasaría",
            "what would happen",
        ],
    ) && primary_task != TaskType::Reasoning
    {
        push_task(&mut tasks, TaskType::Reasoning);
    }

    if contains_any(lower, &["resume", "resumen", "summarize", "summary"])
        && primary_task != TaskType::Summarization
    {
        push_task(&mut tasks, TaskType::Summarization);
    }

    tasks.retain(|task| *task != primary_task);
    tasks
}

pub(crate) fn is_context_dependent_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "continua",
            "continúa",
            "complementa",
            "expande",
            "hazlo mas simple",
            "hazlo más simple",
            "hazlo simple",
            "la respuesta anterior",
            "respuesta anterior",
            "previous answer",
            "continue",
            "expand",
            "make it simpler",
            "make it shorter",
        ],
    )
}

pub(crate) fn is_ideation_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "idea",
            "ideas",
            "brainstorm",
            "propon",
            "propón",
            "sugiere",
            "suggest",
            "negocio",
            "negocios",
            "startup",
        ],
    )
}

pub(crate) fn is_structured_output_request(trimmed: &str, lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "lista",
            "enumera",
            "dame 3",
            "dame tres",
            "3 ideas",
            "tres ideas",
            "all letters",
            "abecedario",
        ],
    ) || (trimmed.chars().any(|c| c.is_ascii_digit())
        && contains_any(lower, &["ideas", "examples", "ejemplos"]))
}

pub(crate) fn is_hybrid_example_prompt(lower: &str) -> bool {
    contains_any(lower, &["ejemplo", "example"])
        && contains_any(lower, &["ejercicio", "exercise"])
        && contains_any(
            lower,
            &["ecuacion", "ecuación", "emc2", "e=mc2", "math", "matem"],
        )
}

pub(crate) fn is_scientific_reasoning_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "que pasaria",
            "qué pasaría",
            "what would happen",
            "agujero negro",
            "black hole",
            "fuerza gravitacional",
            "gravity",
            "gravedad",
        ],
    ) && contains_any(
        lower,
        &[
            "calcula",
            "calculate",
            "ecuaciones",
            "equations",
            "explica",
            "explain",
        ],
    )
}

pub(crate) fn is_conceptual_math_prompt(trimmed: &str, lower: &str) -> bool {
    let explain_cues = contains_any(
        lower,
        &[
            "que es",
            "qué es",
            "explica",
            "explain",
            "como aplicarlos",
            "cómo aplicarlos",
            "how to apply",
            "vida real",
            "real life",
        ],
    );
    let math_concepts = contains_any(
        lower,
        &[
            "derivada",
            "derivative",
            "limite",
            "límite",
            "limites",
            "límites",
            "integral",
            "calculo",
            "cálculo",
            "matematic",
            "mathemat",
        ],
    );
    let strong_solving_markers = trimmed.contains("f(x)")
        || trimmed.contains("dx")
        || trimmed.contains("dy/dx")
        || trimmed.contains("[[")
        || trimmed.contains("]]")
        || (trimmed.chars().any(|c| c.is_ascii_digit())
            && trimmed
                .chars()
                .any(|c| matches!(c, '+' | '-' | '*' | '/' | '=' | '^')));

    explain_cues && math_concepts && !strong_solving_markers
}

pub(crate) fn is_philosophical_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[
            "filosof",
            "dios",
            "justice",
            "justicia",
            "consciousness",
            "conciencia",
            "ética",
            "etica",
        ],
    )
}

pub(crate) fn has_symbolic_math_markers(trimmed: &str, lower: &str) -> bool {
    trimmed.contains("f(x)")
        || trimmed.contains("dx")
        || trimmed.contains("dy/dx")
        || trimmed.contains("[[")
        || trimmed.contains("]]")
        || contains_any(
            lower,
            &[
                "derivada",
                "integral",
                "matriz",
                "matrices",
                "sin(",
                "cos(",
                "tan(",
                "derivative",
                "matrix",
                "limit",
                "limite",
                "límite",
            ],
        )
}

pub(crate) fn has_exact_math_markers(trimmed: &str, lower: &str) -> bool {
    (trimmed.chars().any(|c| c.is_ascii_digit())
        && trimmed
            .chars()
            .any(|c| matches!(c, '+' | '-' | '*' | '/' | '^' | 'x' | 'X')))
        || normalize_expression(trimmed).is_some()
        || contains_any(
            lower,
            &[
                "sqrt(",
                "log",
                "digits of pi",
                "digitos de pi",
                "dígitos de pi",
            ],
        )
}

pub(crate) fn has_math_domain_markers(trimmed: &str, lower: &str) -> bool {
    has_exact_math_markers(trimmed, lower)
        || has_symbolic_math_markers(trimmed, lower)
        || contains_any(
            lower,
            &[
                "matem",
                "math",
                "emc2",
                "e=mc2",
                "einstein",
                "ecuacion",
                "ecuación",
                "funcion",
                "función",
                "derivada",
                "integral",
                "limite",
                "límite",
            ],
        )
}

pub(crate) fn is_multi_intent_prompt(lower: &str) -> bool {
    contains_any(
        lower,
        &[" y ", " and ", " luego ", " then ", " además ", " ademas "],
    )
}

fn push_task(tasks: &mut Vec<TaskType>, task: TaskType) {
    if !tasks.contains(&task) {
        tasks.push(task);
    }
}

pub(crate) fn contains_any(prompt: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| prompt.contains(needle))
}
