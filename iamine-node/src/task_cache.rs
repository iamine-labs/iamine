use std::collections::HashSet;
use std::collections::VecDeque;

/// Cache LRU simple para evitar ejecutar tareas duplicadas
pub struct TaskCache {
    seen: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl TaskCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    /// Retorna true si la tarea ya fue vista (duplicada)
    pub fn is_duplicate(&mut self, task_id: &str) -> bool {
        if self.seen.contains(task_id) {
            return true;
        }

        // Evictar el más antiguo si llegamos al límite
        if self.seen.len() >= self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }

        self.seen.insert(task_id.to_string());
        self.order.push_back(task_id.to_string());
        false
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }
}
