use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct TokenBuffer {
    next_index: usize,
    pending: BTreeMap<usize, String>,
}

impl TokenBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserta token y devuelve todos los tokens consecutivos listos para imprimir
    pub fn push(&mut self, index: usize, token: String) -> Vec<String> {
        self.pending.entry(index).or_insert(token);

        let mut ready = Vec::new();
        while let Some(tok) = self.pending.remove(&self.next_index) {
            ready.push(tok);
            self.next_index += 1;
        }
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::TokenBuffer;

    #[test]
    fn token_ordering_by_index() {
        let mut b = TokenBuffer::new();

        assert!(b.push(2, "c".into()).is_empty());
        assert!(b.push(1, "b".into()).is_empty());

        let out0 = b.push(0, "a".into());
        assert_eq!(out0.join(""), "abc");

        let out1 = b.push(4, "e".into());
        assert!(out1.is_empty());

        let out2 = b.push(3, "d".into());
        assert_eq!(out2.join(""), "de");
    }
}
