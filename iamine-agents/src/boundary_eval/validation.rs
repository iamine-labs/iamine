use std::collections::HashSet;
use std::net::IpAddr;
use std::path::{Component, Path};

use crate::identifiers::{is_package_identifier, is_snake_identifier};

use super::error::{BoundaryEvalViolationCode, BoundaryEvalViolations, ViolationCollector};
use super::schema::{
    BoundaryEvalCase, BoundaryEvalClass, BoundaryEvalRoute, BoundaryEvalSuite,
    BoundaryExpectedAction, BOUNDARY_EVAL_SCHEMA_ID,
};

const MAX_CASES: usize = 64;
const MAX_SYNTHETIC_INPUT_BYTES: usize = 512;
const MAX_REFERENCE_BYTES: usize = 256;

const REQUIRED_CLASSES: [BoundaryEvalClass; 9] = [
    BoundaryEvalClass::InScopePositive,
    BoundaryEvalClass::OutOfScopeNegative,
    BoundaryEvalClass::AmbiguousTask,
    BoundaryEvalClass::DangerousTask,
    BoundaryEvalClass::CrossDomainTask,
    BoundaryEvalClass::PermissionEscalation,
    BoundaryEvalClass::PromptInjection,
    BoundaryEvalClass::RoleConfusion,
    BoundaryEvalClass::HandoffToOrchestrator,
];

const REQUIRED_ACTIONS: [BoundaryExpectedAction; 5] = [
    BoundaryExpectedAction::AllowReviewResponse,
    BoundaryExpectedAction::Refuse,
    BoundaryExpectedAction::Clarify,
    BoundaryExpectedAction::HandoffToOrchestrator,
    BoundaryExpectedAction::RefuseOrHandoff,
];

pub(crate) fn validate_boundary_eval_suite(
    suite: &BoundaryEvalSuite,
) -> Result<(), BoundaryEvalViolations> {
    let mut collector = ViolationCollector::default();

    if suite.schema != BOUNDARY_EVAL_SCHEMA_ID {
        collector.push(
            BoundaryEvalViolationCode::UnsupportedSchema,
            "schema",
            "boundary eval schema identifier is not supported",
        );
    }
    if !is_package_identifier(&suite.package_id) || !suite.package_id.starts_with("iamine.") {
        collector.push(
            BoundaryEvalViolationCode::InvalidIdentifier,
            "package_id",
            "package identifier must be bounded and IAMINE-scoped",
        );
    }
    validate_identifier(&mut collector, "eval_suite_id", &suite.eval_suite_id);
    if semver::Version::parse(&suite.eval_suite_version).is_err() {
        collector.push(
            BoundaryEvalViolationCode::InvalidVersion,
            "eval_suite_version",
            "eval suite version must use semantic versioning",
        );
    }

    validate_reference(&mut collector, "scope_ref", &suite.scope_ref);
    validate_reference(&mut collector, "permission_ref", &suite.permission_ref);
    validate_reference(&mut collector, "audit_ref", &suite.audit_ref);
    validate_required_classes(&mut collector, &suite.required_classes);
    validate_expected_actions(&mut collector, &suite.expected_actions);
    validate_cases(&mut collector, &suite.required_classes, &suite.cases);
    validate_redaction_policy(&mut collector, suite);
    validate_review(&mut collector, suite);

    collector.finish()
}

fn validate_identifier(collector: &mut ViolationCollector, field: &'static str, value: &str) {
    if !is_snake_identifier(value) {
        collector.push(
            BoundaryEvalViolationCode::InvalidIdentifier,
            field,
            "identifier must be bounded lowercase snake case",
        );
    }
}

fn validate_reference(collector: &mut ViolationCollector, field: &'static str, value: &str) {
    let path = Path::new(value);
    let is_safe = !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.is_ascii()
        && !value.contains(['\\', ':', '\0'])
        && !value.starts_with('~')
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));

    if !is_safe {
        collector.push(
            BoundaryEvalViolationCode::InvalidReference,
            field,
            "reference must be a bounded package-relative path",
        );
    }
}

fn validate_required_classes(collector: &mut ViolationCollector, classes: &[BoundaryEvalClass]) {
    if classes.len() != REQUIRED_CLASSES.len() {
        collector.push(
            BoundaryEvalViolationCode::InvalidCollection,
            "required_classes",
            "required boundary eval class declaration is incomplete",
        );
    }

    let unique: HashSet<_> = classes.iter().copied().collect();
    if unique.len() != classes.len() {
        collector.push(
            BoundaryEvalViolationCode::DuplicateValue,
            "required_classes",
            "required boundary eval classes must be unique",
        );
    }
    if REQUIRED_CLASSES
        .iter()
        .any(|required| !unique.contains(required))
    {
        collector.push(
            BoundaryEvalViolationCode::MissingRequiredClass,
            "required_classes",
            "all fail-closed boundary eval classes must be declared",
        );
    }
}

fn validate_expected_actions(
    collector: &mut ViolationCollector,
    actions: &[BoundaryExpectedAction],
) {
    if actions.len() != REQUIRED_ACTIONS.len() {
        collector.push(
            BoundaryEvalViolationCode::InvalidCollection,
            "expected_actions",
            "expected action declaration is incomplete",
        );
    }

    let unique: HashSet<_> = actions.iter().copied().collect();
    if unique.len() != actions.len() {
        collector.push(
            BoundaryEvalViolationCode::DuplicateValue,
            "expected_actions",
            "expected actions must be unique",
        );
    }
    if REQUIRED_ACTIONS
        .iter()
        .any(|required| !unique.contains(required))
    {
        collector.push(
            BoundaryEvalViolationCode::MissingSafetyBoundary,
            "expected_actions",
            "all bounded review actions must be declared",
        );
    }
}

fn validate_cases(
    collector: &mut ViolationCollector,
    required_classes: &[BoundaryEvalClass],
    cases: &[BoundaryEvalCase],
) {
    if cases.is_empty() || cases.len() > MAX_CASES {
        collector.push(
            BoundaryEvalViolationCode::InvalidCollection,
            "cases",
            "boundary eval cases must be bounded and non-empty",
        );
    }

    let required: HashSet<_> = required_classes.iter().copied().collect();
    let mut case_ids = HashSet::with_capacity(cases.len());
    let mut covered_classes = HashSet::with_capacity(cases.len());

    for case in cases {
        validate_identifier(collector, "cases.case_id", &case.case_id);
        if !case_ids.insert(case.case_id.as_str()) {
            collector.push(
                BoundaryEvalViolationCode::DuplicateValue,
                "cases.case_id",
                "boundary eval case identifiers must be unique",
            );
        }
        if !required.contains(&case.class) {
            collector.push(
                BoundaryEvalViolationCode::ContradictoryExpectation,
                "cases.class",
                "case class must be present in required classes",
            );
        }
        covered_classes.insert(case.class);
        validate_synthetic_input(collector, &case.synthetic_input);
        validate_case_expectation(collector, case);
    }

    if REQUIRED_CLASSES
        .iter()
        .any(|required| !covered_classes.contains(required))
    {
        collector.push(
            BoundaryEvalViolationCode::MissingRequiredClass,
            "cases",
            "every required class must have at least one synthetic case",
        );
    }
}

fn validate_synthetic_input(collector: &mut ViolationCollector, input: &str) {
    if input.trim().is_empty()
        || input.len() > MAX_SYNTHETIC_INPUT_BYTES
        || !input.is_ascii()
        || input.chars().any(char::is_control)
    {
        collector.push(
            BoundaryEvalViolationCode::InvalidSyntheticInput,
            "cases.synthetic_input",
            "synthetic input must be bounded single-line ASCII text",
        );
        return;
    }

    if contains_private_shape(input) {
        collector.push(
            BoundaryEvalViolationCode::PrivacyViolation,
            "cases.synthetic_input",
            "synthetic input contains a prohibited private-data shape",
        );
    }
}

fn contains_private_shape(input: &str) -> bool {
    let lowercase = input.to_ascii_lowercase();
    let has_private_path = [
        "/users/",
        "/home/",
        "\\users\\",
        "\\documents and settings\\",
        "~/",
    ]
    .iter()
    .any(|marker| lowercase.contains(marker));
    let has_secret_assignment = [
        "api_key=",
        "password=",
        "private_key=",
        "secret=",
        "token=",
        "-----begin private key-----",
    ]
    .iter()
    .any(|marker| lowercase.contains(marker));
    let has_email = input
        .split_ascii_whitespace()
        .any(|token| token.contains('@') && token.contains('.'));
    let has_ip = input.split_ascii_whitespace().any(|token| {
        token
            .trim_matches(|ch: char| !ch.is_ascii_hexdigit() && ch != '.' && ch != ':')
            .parse::<IpAddr>()
            .is_ok()
    });
    let has_mac = input
        .split_ascii_whitespace()
        .map(|token| {
            token.trim_matches(|ch: char| !ch.is_ascii_hexdigit() && ch != ':' && ch != '-')
        })
        .any(is_mac_address);

    has_private_path || has_secret_assignment || has_email || has_ip || has_mac
}

fn is_mac_address(value: &str) -> bool {
    let separator = if value.contains(':') {
        ':'
    } else if value.contains('-') {
        '-'
    } else {
        return false;
    };
    let segments: Vec<_> = value.split(separator).collect();
    segments.len() == 6
        && segments
            .iter()
            .all(|segment| segment.len() == 2 && segment.chars().all(|ch| ch.is_ascii_hexdigit()))
}

fn validate_case_expectation(collector: &mut ViolationCollector, case: &BoundaryEvalCase) {
    let action_matches_class = match case.class {
        BoundaryEvalClass::InScopePositive => {
            case.expected_action == BoundaryExpectedAction::AllowReviewResponse
        }
        BoundaryEvalClass::AmbiguousTask => matches!(
            case.expected_action,
            BoundaryExpectedAction::Clarify | BoundaryExpectedAction::HandoffToOrchestrator
        ),
        BoundaryEvalClass::CrossDomainTask | BoundaryEvalClass::HandoffToOrchestrator => {
            case.expected_action == BoundaryExpectedAction::HandoffToOrchestrator
        }
        BoundaryEvalClass::OutOfScopeNegative
        | BoundaryEvalClass::DangerousTask
        | BoundaryEvalClass::PermissionEscalation
        | BoundaryEvalClass::PromptInjection
        | BoundaryEvalClass::RoleConfusion => matches!(
            case.expected_action,
            BoundaryExpectedAction::Refuse
                | BoundaryExpectedAction::HandoffToOrchestrator
                | BoundaryExpectedAction::RefuseOrHandoff
        ),
    };
    let route_matches_action = match case.expected_action {
        BoundaryExpectedAction::AllowReviewResponse
        | BoundaryExpectedAction::Refuse
        | BoundaryExpectedAction::Clarify => case.expected_route == BoundaryEvalRoute::Agent,
        BoundaryExpectedAction::HandoffToOrchestrator | BoundaryExpectedAction::RefuseOrHandoff => {
            case.expected_route == BoundaryEvalRoute::Orchestrator
        }
    };

    if !action_matches_class || !route_matches_action {
        collector.push(
            BoundaryEvalViolationCode::ContradictoryExpectation,
            "cases.expected_action",
            "case action and route must preserve its fail-closed class boundary",
        );
    }
}

fn validate_redaction_policy(collector: &mut ViolationCollector, suite: &BoundaryEvalSuite) {
    let policy = &suite.redaction_policy;
    if !policy.synthetic_inputs_only
        || !policy.blocks_raw_user_prompts
        || !policy.blocks_raw_outputs
        || !policy.blocks_private_paths
        || !policy.blocks_host_identifiers
        || !policy.blocks_credentials
    {
        collector.push(
            BoundaryEvalViolationCode::MissingSafetyBoundary,
            "redaction_policy",
            "boundary evals must require every privacy-safe redaction boundary",
        );
    }
}

fn validate_review(collector: &mut ViolationCollector, suite: &BoundaryEvalSuite) {
    if !suite.review.requires_human_review
        || !suite.review.requires_qa_evidence
        || suite.review.self_approval_allowed
    {
        collector.push(
            BoundaryEvalViolationCode::MissingSafetyBoundary,
            "review",
            "boundary eval metadata must require independent human and QA review",
        );
    }
    if suite.review.evidence.is_empty() || suite.review.evidence.len() > MAX_CASES {
        collector.push(
            BoundaryEvalViolationCode::InvalidCollection,
            "review.evidence",
            "review evidence references must be bounded and non-empty",
        );
    }

    let mut unique = HashSet::with_capacity(suite.review.evidence.len());
    for reference in &suite.review.evidence {
        validate_reference(collector, "review.evidence", reference);
        if !unique.insert(reference.as_str()) {
            collector.push(
                BoundaryEvalViolationCode::DuplicateValue,
                "review.evidence",
                "review evidence references must be unique",
            );
        }
    }
}
