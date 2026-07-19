pub(crate) const MAX_IDENTIFIER_BYTES: usize = 128;

pub(crate) fn is_package_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.is_ascii()
        && value.split(['.', '-']).all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        })
}

pub(crate) fn is_snake_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.is_ascii()
        && !value.starts_with('_')
        && !value.ends_with('_')
        && !value.contains("__")
        && value
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}
