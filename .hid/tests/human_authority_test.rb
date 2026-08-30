# frozen_string_literal: true

require_relative "test_helper"

class HidHumanAuthorityTest < HidTestCase
  def test_passed_human_gate_without_event_fails
    assert_raises(Hid::ValidationError) { @validator.validate_human_gates(@feature, [], @project) }
  end

  def test_agent_cannot_be_human_authorization_actor
    event = authorization_event
    event["actor"]["type"] = "agent"

    assert_raises(Hid::ValidationError) do
      @validator.validate_human_authorization_event(event, @project, "fixture")
    end
  end

  def test_authorization_for_wrong_tree_does_not_support_gate
    event = authorization_event
    event["artifact"]["tree"] = OTHER_TREE

    assert_raises(Hid::ValidationError) { @validator.validate_human_gates(@feature, [event], @project) }
  end

  def test_matching_human_authorization_supports_gate
    @validator.validate_human_authorization_event(authorization_event, @project, "fixture")
    @validator.validate_human_gates(@feature, [authorization_event], @project)
  end

  def test_historical_snapshot_authorization_does_not_authorize_current_candidate
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)

    error = assert_raises(Hid::ValidationError) do
      @validator.validate_human_gates(@feature, [authorization_event], @project, current)
    end
    assert_includes error.message, "stale"
  end

  def test_current_candidate_authorization_supports_gate
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    event = authorization_event(head: OTHER_HEAD, tree: OTHER_TREE)

    @validator.validate_human_gates(@feature, [event], @project, current)
  end

  def test_approval_followed_by_denial_is_not_authorized
    events = [authorization_event, authorization_event(decision: "denied")]

    error = assert_raises(Hid::ValidationError) do
      @validator.validate_human_gates(@feature, events, @project)
    end
    assert_includes error.message, "denied"
  end

  def test_denial_followed_by_approval_is_authorized
    events = [authorization_event(decision: "denied"), authorization_event]

    @validator.validate_human_gates(@feature, events, @project)
  end

  def test_new_artifact_approval_supersedes_old_artifact_approval
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    events = [authorization_event, authorization_event(head: OTHER_HEAD, tree: OTHER_TREE)]

    @validator.validate_human_gates(@feature, events, @project, current)
  end

  def test_privileged_state_rejects_authorizations_for_historical_snapshot
    feature = state_feature("APPROVED FOR MERGE")
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)

    error = assert_raises(Hid::ValidationError) do
      validate_state(feature, authorization_events, current: current)
    end
    assert_includes error.message, "AUTHORIZATION_STALE"
    assert_equal "state_gate_inconsistency", derive_next_action(feature, authorization_events, current: current)
  end

  def test_privileged_state_accepts_current_authorizations_with_historical_evidence
    feature = state_feature("APPROVED FOR MERGE")
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    events = authorization_events(head: OTHER_HEAD, tree: OTHER_TREE)

    validate_state(feature, events, current: current)
    assert_equal "run_merge_precheck", derive_next_action(feature, events, current: current)
  end

  def test_authorization_action_must_match_gate
    event = authorization_event
    event["authorization"]["action"] = "release"

    assert_raises(Hid::ValidationError) do
      @validator.validate_human_authorization_event(event, @project, "fixture")
    end
  end

  def test_approved_for_merge_with_pending_gates_fails
    feature = state_feature("APPROVED FOR MERGE", "final_review" => "pending", "human_merge" => "pending")

    assert_raises(Hid::ValidationError) { validate_state(feature, []) }
    assert_equal "state_gate_inconsistency", derive_next_action(feature, [])
  end

  def test_approved_for_merge_with_one_gate_missing_fails
    feature = state_feature("APPROVED FOR MERGE", "human_merge" => "pending")
    events = authorization_events.reject { |event| event.dig("authorization", "gate") == "human_merge" }

    assert_raises(Hid::ValidationError) { validate_state(feature, events) }
  end

  def test_approved_for_merge_with_passed_gates_but_no_authorization_fails
    assert_raises(Hid::ValidationError) { validate_state(state_feature("APPROVED FOR MERGE"), []) }
  end

  def test_approved_for_merge_with_all_requirements_passes
    feature = state_feature("APPROVED FOR MERGE")

    validate_state(feature, authorization_events)
    assert_equal "run_merge_precheck", derive_next_action(feature, authorization_events)
  end

  def test_merged_state_without_merged_event_fails
    assert_raises(Hid::ValidationError) { validate_state(state_feature("MERGED"), authorization_events) }
  end

  def test_closed_state_without_post_merge_events_fails
    feature = state_feature("MERGED / VALIDATED / CLOSED")
    events = authorization_events + [workflow_event("merged")]

    assert_raises(Hid::ValidationError) { validate_state(feature, events) }
  end
end
