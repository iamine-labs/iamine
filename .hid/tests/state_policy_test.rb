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

  private

  def candidate_evidence
    {
      "HID-EVID-0001" => {
        "derived_status" => "VALID",
        "artifact" => {"head_sha" => HEAD, "tree" => TREE}
      }
    }
  end
end
