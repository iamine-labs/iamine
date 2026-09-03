# frozen_string_literal: true

require_relative "operational_test_helper"

class HidGateProjectionTest < HidTestCase
  include OperationalFixture

  def test_real_manifest_starts_without_operational_authority
    feature = operational_feature
    assert_equal "passed", feature.dig("gates", "local_validation", "status")
    result = operational_projection
    assert_equal "pending", result.dig("gates", "local_validation", "status")
    assert_equal "ARCHITECTURE IN PROGRESS", result["state"]
    assert_equal "request_architecture_review", result["next_action"]
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
  end

  def test_legacy_gate_and_state_mutations_cannot_promote
    original = operational_projection
    feature = operational_feature
    feature["gates"].each_value { |gate| gate["status"] = "passed" }
    feature["state"]["current"] = "APPROVED FOR MERGE"
    changed = operational_projection([], feature: feature)
    %w[gates state next_action human_gate_eligibility].each { |key| assert_equal original[key], changed[key] }
    feature["state"]["current"] = "not-an-authority"
    assert_equal original["state"], operational_projection([], feature: feature)["state"]
  end

  def test_canonical_progression_and_capsule_eligibility
    events = []
    expected = %w[implement_feature run_local_validation request_architecture_checkpoint request_final_review request_human_gate]
    nonhuman_events.each_with_index do |event, index|
      events << event
      result = operational_projection(events)
      assert_equal expected[index], result["next_action"]
      assert_equal index == 4 ? "READY" : "NOT_READY", result.dig("human_gate_eligibility", "status")
    end
    capsule = capsule_event(events)
    events += [capsule, human_event(capsule)]
    result = operational_projection(events)
    assert_equal "APPROVED FOR MERGE", result["state"]
    assert_equal "run_merge_precheck", result["next_action"]
    assert_equal "passed", result.dig("gates", "human_merge", "status")
    assert_empty result["failures"]
  end

  def test_stale_reviews_and_validation_cannot_support_new_candidate
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    result = operational_projection(approved_events, current: current)
    %w[architecture local_validation final_review human_merge].each do |gate|
      assert_equal "pending", result.dig("gates", gate, "status")
    end
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
  end

  def test_cross_feature_events_cannot_support_same_artifact
    project = state_project
    project["operational_policy"]["mandates"].each_value { |mandate| mandate["features"] << "OTHER-FEATURE-001" }
    events = nonhuman_events.each do |event|
      event["feature"] = "OTHER-FEATURE-001"
      event["outcome"]["evidence"]["feature"] = "OTHER-FEATURE-001"
    end
    assert_equal "pending", operational_projection(events, project: project).dig("gates", "final_review", "status")
  end

  def test_negative_review_blocks_and_latest_review_can_reapprove
    events = nonhuman_events
    failure = operational_event("final_review", result: "fail")
    failure["ts"] = "2020-01-01T00:00:00Z"
    events << failure
    blocked = operational_projection(events)
    assert_equal "CHANGES REQUIRED", blocked["state"]
    assert_equal "resolve_final_review", blocked["next_action"]
    assert_equal "NOT_READY", blocked.dig("human_gate_eligibility", "status")
    events << operational_event("final_review")
    assert_equal "READY", operational_projection(events).dig("human_gate_eligibility", "status")
  end

  def test_human_merge_cannot_substitute_reviews
    result = operational_projection([human_event({"id" => "HID-EVENT-9999"})])
    assert_equal "pending", result.dig("gates", "architecture", "status")
    assert_equal "pending", result.dig("gates", "final_review", "status")
    refute_equal "APPROVED FOR MERGE", result["state"]
  end

  def test_premature_capsule_is_invalid_even_when_gates_pass_later
    early = capsule_event([])
    result = operational_projection([early] + nonhuman_events + [human_event(early)])
    assert result["failures"].any? { |failure| failure.start_with?("APPROVAL_CAPSULE_NOT_READY") }
    refute_equal "APPROVED FOR MERGE", result["state"]
  end

  def test_new_review_after_approval_requires_new_capsule
    events = approved_events + [operational_event("final_review")]
    assert_equal "blocked", operational_projection(events).dig("gates", "human_merge", "status")
    capsule = capsule_event(events)
    events += [capsule, human_event(capsule)]
    assert_equal "APPROVED FOR MERGE", operational_projection(events)["state"]
  end

  def test_latest_human_denial_and_reapproval
    events = approved_events
    denial = human_event(decision: "denied")
    denial["ts"] = "2020-01-01T00:00:00Z"
    events << denial
    assert_equal "failed", operational_projection(events).dig("gates", "human_merge", "status")
    capsule = capsule_event(events)
    assert_equal "APPROVED FOR MERGE", operational_projection(events + [capsule, human_event(capsule)])["state"]
  end

  def test_out_of_order_review_does_not_unlock_capsule
    events = nonhuman_events
    events[2], events[4] = events[4], events[2]
    assert_equal "NOT_READY", operational_projection(events).dig("human_gate_eligibility", "status")
  end

  def test_unknown_requirement_fails_closed
    feature = operational_feature
    feature["gates"]["local_validation"].delete("required")
    result = operational_projection(nonhuman_events, feature: feature)
    assert_equal "unknown", result.dig("gates", "local_validation", "status")
    assert_equal "policy_incomplete", result["next_action"]
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
  end

  def test_non_required_gate_event_cannot_add_authority
    events = nonhuman_events
    events.insert(4, operational_event("field_qa"))
    result = operational_projection(events)
    assert_equal "not_required", result.dig("gates", "field_qa", "status")
    refute_includes result.dig("human_gate_eligibility", "prerequisite_events"), events[4]["id"]
  end

  def test_required_field_qa_blocks_until_fresh_fact
    feature = operational_feature
    feature["gates"]["field_qa"]["required"] = true
    result = operational_projection(nonhuman_events, feature: feature)
    assert_equal "run_field_qa", result["next_action"]
    assert_equal "READY", operational_projection(nonhuman_events(field_qa: true), feature: feature).dig("human_gate_eligibility", "status")
  end

  def test_manifest_cannot_weaken_constitutional_gate
    feature = operational_feature
    feature["gates"]["final_review"]["required"] = false
    result = operational_projection(nonhuman_events.take(4), feature: feature)
    assert_equal true, result.dig("gates", "final_review", "required")
    assert_equal "request_final_review", result["next_action"]
  end

  def test_legacy_human_authorization_is_history_not_new_authority
    legacy = authorization_event.merge("schema_version" => "0.0.2", "id" => "HID-EVENT-0060")
    rule = state_project.dig("human_gates", "human_merge")
    git = FakeGit.new(HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE])
    old = Hid::StateInvariants.new(operational_feature, [legacy], state_project, current: current_candidate, git: git)
    assert_equal :approved, old.authorization_status("human_merge", rule)
    newer = Hid::StateInvariants.new(operational_feature, [legacy], state_project, current: current_candidate(head: OTHER_HEAD, tree: OTHER_TREE), git: git)
    assert_equal :stale, newer.authorization_status("human_merge", rule)
    assert_equal "pending", operational_projection([legacy]).dig("gates", "human_merge", "status")
  end

  def test_dirty_candidate_cannot_be_capsule_ready
    assert_equal "NOT_READY", operational_projection(approved_events, current: current_candidate(dirty: true)).dig("human_gate_eligibility", "status")
  end
end
