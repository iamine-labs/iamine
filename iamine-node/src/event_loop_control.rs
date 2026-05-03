#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EventLoopDirective {
    None,
    Continue,
    Break,
}
