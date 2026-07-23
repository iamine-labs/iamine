use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::validation::validate_boundary_eval_suite;
use super::{json_schema, parse_yaml, BoundaryEvalError};

pub const BOUNDARY_EVAL_SCHEMA_ID: &str = "iamine.agent.boundary_evals.draft-0.1";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryEvalSuite {
    pub schema: String,
    pub package_id: String,
    pub eval_suite_id: String,
    pub eval_suite_version: String,
    pub scope_ref: String,
    pub permission_ref: String,
    pub audit_ref: String,
    pub required_classes: Vec<BoundaryEvalClass>,
    pub cases: Vec<BoundaryEvalCase>,
    pub expected_actions: Vec<BoundaryExpectedAction>,
    pub forbidden_successes: BoundaryForbiddenSuccesses,
    pub redaction_policy: BoundaryRedactionPolicy,
    pub failure_policy: BoundaryFailurePolicy,
    pub review: BoundaryEvalReview,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryEvalCase {
    pub case_id: String,
    pub class: BoundaryEvalClass,
    pub synthetic_input: String,
    pub expected_action: BoundaryExpectedAction,
    pub expected_route: BoundaryEvalRoute,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryEvalClass {
    InScopePositive,
    OutOfScopeNegative,
    AmbiguousTask,
    DangerousTask,
    CrossDomainTask,
    PermissionEscalation,
    PromptInjection,
    RoleConfusion,
    HandoffToOrchestrator,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryExpectedAction {
    AllowReviewResponse,
    Refuse,
    Clarify,
    HandoffToOrchestrator,
    RefuseOrHandoff,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryEvalRoute {
    Agent,
    Orchestrator,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryBlockPolicy {
    Block,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryForbiddenSuccesses {
    pub allows_blocked_action: BoundaryBlockPolicy,
    pub expands_scope: BoundaryBlockPolicy,
    pub grants_permission: BoundaryBlockPolicy,
    pub collects_private_data: BoundaryBlockPolicy,
    pub claims_runtime_execution: BoundaryBlockPolicy,
    pub claims_registry_publication: BoundaryBlockPolicy,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryRedactionPolicy {
    pub synthetic_inputs_only: bool,
    pub blocks_raw_user_prompts: bool,
    pub blocks_raw_outputs: bool,
    pub blocks_private_paths: bool,
    pub blocks_host_identifiers: bool,
    pub blocks_credentials: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryFailurePolicy {
    pub missing_eval_suite: BoundaryBlockPolicy,
    pub missing_required_class: BoundaryBlockPolicy,
    pub unsafe_expected_action: BoundaryBlockPolicy,
    pub unredacted_evidence: BoundaryBlockPolicy,
    pub contradictory_scope_result: BoundaryBlockPolicy,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundaryEvalReview {
    pub requires_human_review: bool,
    pub requires_qa_evidence: bool,
    pub self_approval_allowed: bool,
    pub evidence: Vec<String>,
}

pub fn boundary_eval_json_schema() -> Result<serde_json::Value, BoundaryEvalError> {
    json_schema::<BoundaryEvalSuite>()
}

pub fn parse_boundary_eval_yaml(input: &str) -> Result<BoundaryEvalSuite, BoundaryEvalError> {
    parse_yaml(input, validate_boundary_eval_suite)
}
