# frozen_string_literal: true

require_relative "operational_test_helper"

class HidReviewAuthorityTest < HidTestCase
  include OperationalFixture

  def test_policy_authorized_agent_can_review_without_human_identity
    event = operational_event("final_review")
    Hid::OperationalFacts.new(state_project).validate!(event)
    assert_equal "agent", event.dig("actor", "type")
    refute event.key?("authorization")
  end

  def test_wrong_role_mandate_phase_result_and_evidence_are_rejected
    mutations = [
      ->(event) { event["actor"]["role"] = "developer" },
      ->(event) { event["outcome"].delete("mandate") },
      ->(event) { event["outcome"]["mandate"] = "arbitrary-architect" },
      ->(event) { event["outcome"]["phase"] = "development_authorization" },
      ->(event) { event["outcome"]["result"] = "approved" },
      ->(event) { event["outcome"].delete("evidence") },
      ->(event) { event["outcome"]["evidence"]["artifact"]["tree"] = OTHER_TREE },
      ->(event) { event["outcome"]["evidence"]["feature"] = "OTHER-FEATURE-001" },
      ->(event) { event["event"] = "final_review_passed" }
    ]
    mutations.each do |mutate|
      event = operational_event("final_review")
      mutate.call(event)
      assert_raises(Hid::ValidationError) { Hid::OperationalFacts.new(state_project).validate!(event) }
    end
  end

  def test_external_validation_requires_exact_evidence_and_validation_type
    event = operational_event("local_validation")
    Hid::OperationalFacts.new(state_project).validate!(event)
    event["outcome"]["evidence"]["kind"] = "final_review"
    assert_raises(Hid::ValidationError) { Hid::OperationalFacts.new(state_project).validate!(event) }
  end

  def test_constitution_rejects_downgraded_review_authority
    project = Marshal.load(Marshal.dump(state_project))
    project["operational_policy"]["gate_authorities"].delete("final_review")
    assert Hid::OperationalFacts.new(project).policy_failures.any? { |message| message.start_with?("CONSTITUTIONAL_POLICY_VIOLATION") }
  end

  def test_constitution_rejects_human_review_conflation
    project = Marshal.load(Marshal.dump(state_project))
    project["human_gates"]["final_review"] = {"action" => "architecture_merge_approval", "artifact_bound" => true}
    refute_empty Hid::OperationalFacts.new(project).policy_failures
  end

  def test_agent_review_actor_cannot_approve_human_merge
    event = human_event({"id" => "HID-EVENT-0100"})
    event["actor"] = {"type" => "agent", "role" => "architect"}
    assert_raises(Hid::ValidationError) { Hid::OperationalFacts.new(state_project).validate!(event) }
  end

  def test_nominal_architect_without_policy_permission_is_rejected
    project = Marshal.load(Marshal.dump(state_project))
    project["operational_policy"]["mandates"]["iamine-architecture"]["features"] = ["OTHER-FEATURE-001"]
    assert_raises(Hid::ValidationError) { Hid::OperationalFacts.new(project).validate!(operational_event("final_review")) }
  end
end
