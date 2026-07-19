use crate::{PermissionDecision, PermissionEvaluation, ScopeDecision, ScopeEvaluation};

use super::{
    AgentAuditEvent, AuditEventClass, AuditEventSet, AuditEventSource, AuditOutcome,
    AuditReasonCode,
};

pub const fn audit_scope_evaluation(evaluation: &ScopeEvaluation) -> AuditEventSet {
    let outcome = match evaluation.decision() {
        ScopeDecision::Allow => AuditOutcome::Allowed,
        ScopeDecision::Clarify => AuditOutcome::ClarificationRequired,
        ScopeDecision::Refuse => AuditOutcome::Refused,
        ScopeDecision::HandoffToOrchestrator => AuditOutcome::HandedOff,
    };
    let checked = AgentAuditEvent::new(
        AuditEventClass::ScopeChecked,
        AuditEventSource::Scope,
        outcome,
        AuditReasonCode::Scope(evaluation.reason()),
        None,
    );

    match evaluation.decision() {
        ScopeDecision::Refuse => AuditEventSet::pair(
            checked,
            decision_event(
                AuditEventClass::RefusalRecorded,
                AuditEventSource::Scope,
                AuditReasonCode::Scope(evaluation.reason()),
            ),
        ),
        ScopeDecision::HandoffToOrchestrator => AuditEventSet::pair(
            checked,
            decision_event(
                AuditEventClass::HandoffRequired,
                AuditEventSource::Scope,
                AuditReasonCode::Scope(evaluation.reason()),
            ),
        ),
        ScopeDecision::Allow | ScopeDecision::Clarify => AuditEventSet::single(checked),
    }
}

pub const fn audit_permission_evaluation(evaluation: &PermissionEvaluation) -> AuditEventSet {
    let outcome = match evaluation.decision() {
        PermissionDecision::Allow => AuditOutcome::Allowed,
        PermissionDecision::RequireConfirmation => AuditOutcome::ConfirmationRequired,
        PermissionDecision::Refuse => AuditOutcome::Refused,
        PermissionDecision::HandoffToOrchestrator => AuditOutcome::HandedOff,
    };
    let checked = AgentAuditEvent::new(
        AuditEventClass::PermissionChecked,
        AuditEventSource::Permission,
        outcome,
        AuditReasonCode::Permission(evaluation.reason()),
        None,
    );

    match evaluation.decision() {
        PermissionDecision::Refuse => AuditEventSet::pair(
            checked,
            decision_event(
                AuditEventClass::RefusalRecorded,
                AuditEventSource::Permission,
                AuditReasonCode::Permission(evaluation.reason()),
            ),
        ),
        PermissionDecision::HandoffToOrchestrator => AuditEventSet::pair(
            checked,
            decision_event(
                AuditEventClass::HandoffRequired,
                AuditEventSource::Permission,
                AuditReasonCode::Permission(evaluation.reason()),
            ),
        ),
        PermissionDecision::Allow | PermissionDecision::RequireConfirmation => {
            AuditEventSet::single(checked)
        }
    }
}

const fn decision_event(
    class: AuditEventClass,
    source: AuditEventSource,
    reason: AuditReasonCode,
) -> AgentAuditEvent {
    let outcome = match class {
        AuditEventClass::RefusalRecorded => AuditOutcome::Refused,
        AuditEventClass::HandoffRequired => AuditOutcome::HandedOff,
        AuditEventClass::LifecycleObserved
        | AuditEventClass::ScopeChecked
        | AuditEventClass::PermissionChecked => AuditOutcome::Observed,
    };
    AgentAuditEvent::new(class, source, outcome, reason, None)
}
