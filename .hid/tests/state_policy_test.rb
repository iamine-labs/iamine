# frozen_string_literal: true

require_relative "test_helper"

class HidStatePolicyTest < HidTestCase
  def test_missing_privileged_state_policy_fails_closed
    project = Marshal.load(Marshal.dump(state_project))
    project["state_requirements"].delete("APPROVED FOR MERGE")
    feature = state_feature(
      "APPROVED FOR MERGE",
      "architecture" => "pending",
      "final_review" => "pending",
      "human_merge" => "pending"
    )

    error = assert_raises(Hid::ValidationError) do
      @validator.send(
        :validate_state_and_gates,
        feature,
        candidate_evidence,
        [],
        project,
        current_candidate
      )
    end
    assert_includes error.message, "POLICY_INCOMPLETE"
    assert_equal "policy_incomplete", derive_next_action(feature, [], project: project)
  end

  def test_global_policy_coverage_reports_every_missing_privileged_state
    requirements = state_project.fetch("state_requirements").dup
    requirements.delete("MERGED")
    failures = Hid::StateInvariants.policy_coverage_failures(
      state_project.fetch("derived_privileged_states"),
      requirements
    )

    assert_equal ["POLICY_INCOMPLETE state MERGED has no requirements"], failures
  end

  def test_partial_policy_cannot_omit_constitutional_human_merge
    project = deep_copy(state_project)
    policy = project.dig("state_requirements", "APPROVED FOR MERGE")
    policy.delete("required_gates")
    policy.fetch("gates").delete("human_merge")
    feature = state_feature("APPROVED FOR MERGE", "architecture" => "pending", "human_merge" => "pending")
    feature["gates"]["human_merge"]["required"] = false
    events = authorization_events.reject { |event| event.dig("authorization", "gate") == "human_merge" }

    error = assert_raises(Hid::ValidationError) do
      validate_state_with_project(feature, events, project)
    end
    assert_includes error.message, "gate human_merge must be passed"
    refute_equal "run_merge_precheck", derive_next_action(feature, events, project: project)
  end

  def test_policy_cannot_explicitly_disable_constitutional_human_merge
    project = deep_copy(state_project)
    project.dig("state_requirements", "APPROVED FOR MERGE", "gates")["human_merge"] = false
    feature = state_feature("APPROVED FOR MERGE")

    error = assert_raises(Hid::ValidationError) do
      validate_state_with_project(feature, authorization_events, project)
    end
    assert_includes error.message, "CONSTITUTIONAL_POLICY_VIOLATION"
    assert_equal "constitutional_policy_violation", derive_next_action(feature, authorization_events, project: project)
  end

  def test_project_policy_can_add_a_nonconstitutional_gate
    project = deep_copy(state_project)
    project.dig("state_requirements", "APPROVED FOR MERGE", "gates")["security_review"] = "passed"
    feature = state_feature("APPROVED FOR MERGE")
    feature["gates"]["security_review"] = {"required" => true, "status" => "passed"}

    validate_state_with_project(feature, authorization_events, project)
  end

  def test_constitutional_human_gate_rule_cannot_be_removed
    project = deep_copy(state_project)
    project.fetch("human_gates").delete("human_merge")

    error = assert_raises(Hid::ValidationError) do
      validate_state_with_project(state_feature("APPROVED FOR MERGE"), authorization_events, project)
    end
    assert_includes error.message, "CONSTITUTIONAL_POLICY_VIOLATION"
  end

  private

  def candidate_evidence
    {
      "HID-EVID-0001" => {
        "derived_status" => "VALID",
        "artifact" => {"head_sha" => HEAD, "tree" => TREE}
      }
    }
  end

  def deep_copy(value)
    Marshal.load(Marshal.dump(value))
  end

  def validate_state_with_project(feature, events, project)
    @validator.send(
      :validate_state_and_gates,
      feature,
      candidate_evidence,
      events,
      project,
      current_candidate
    )
  end
end
