# frozen_string_literal: true

require_relative "operational_test_helper"

class HidTypedOperationalFactTest < HidTestCase
  include OperationalFixture

  def build(event, project: state_project)
    Hid::OperationalFacts.new(project, git: FakeGit.new(HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE])).build(event)
  end

  def test_event_type_fixes_each_domain_and_gate
    expected = {
      "architecture" => "REVIEW_VERDICT", "implementation" => "LIFECYCLE_FACT",
      "local_validation" => "VALIDATION_RESULT", "architecture_checkpoint" => "REVIEW_VERDICT",
      "field_qa" => "QA_RESULT", "final_review" => "REVIEW_VERDICT",
      "post_merge_validation" => "VALIDATION_RESULT", "closure" => "LIFECYCLE_FACT"
    }
    expected.each do |gate, domain|
      event = operational_event(gate)
      fact = build(event)
      assert_instance_of Hid::OperationalFact, fact
      assert_equal domain, fact.domain
      assert_equal gate, fact.gate
      assert_equal "pass", fact.result
      assert_equal event["outcome"]["evidence"], fact.evidence
      assert_equal event["id"], fact.id
    end
    event = operational_event("merged")
    event["integration"] = {"source_head_sha" => OTHER_HEAD, "source_tree" => OTHER_TREE,
                            "target_branch" => "develop", "strategy" => "no_ff_merge"}
    assert_equal "INTEGRATION_FACT", build(event).domain
  end

  def test_human_decision_uses_only_canonical_authorization_result
    %w[approved denied].each do |decision|
      fact = build(human_event({"id" => "HID-EVENT-0099"}, decision: decision))
      assert_equal "HUMAN_DECISION", fact.domain
      assert_equal "human_merge", fact.gate
      assert_equal decision, fact.result
      assert_nil fact.evidence
    end
  end

  def test_capsule_request_does_not_emit_gate_authority
    fact = build(capsule_event(nonhuman_events))
    assert_equal "CAPSULE_REQUEST", fact.domain
    assert_nil fact.gate
    assert_nil fact.result
  end

  def test_fact_is_an_immutable_detached_snapshot
    event = operational_event("final_review")
    original = Marshal.dump(event)
    fact = build(event)
    assert_equal original, Marshal.dump(event)
    assert fact.frozen?
    assert_raises(FrozenError) { fact.domain = "HUMAN_DECISION" }
    assert_raises(FrozenError) { fact.subject["tree"] = OTHER_TREE }
    assert_raises(FrozenError) { fact.evidence["reference"].replace("other") }
    event["outcome"]["result"] = "fail"
    assert_equal "pass", fact.result
    assert_equal "pass", fact.source_event.dig("outcome", "result")
  end

  def test_reserved_fields_cannot_self_declare_domain_or_result
    %w[domain authority_domain authority_kind review_type review_phase validation_result qa_result result verdict decision authorization capsule integration].each do |field|
      event = operational_event("final_review")
      event[field] = "HUMAN_DECISION"
      assert_raises(Hid::ValidationError, field) { build(event) }
    end
  end

  def test_nested_contradictory_semantics_are_rejected
    events = [operational_event("final_review"), operational_event("local_validation"), human_event(decision: "denied")]
    events[0]["outcome"]["verdict"] = "CHANGES_REQUIRED"
    events[1]["outcome"]["validation_result"] = "fail"
    events[2]["authorization"]["result"] = "pass"
    events.each { |event| assert_raises(Hid::ValidationError) { build(event) } }
  end

  def test_event_name_and_canonical_result_cannot_contradict
    cases = [
      operational_event("final_review", result: "fail").merge("event" => "architecture_approved"),
      operational_event("final_review").merge("event" => "architecture_changes_required"),
      operational_event("local_validation", result: "fail").merge("event" => "validation_passed"),
      operational_event("local_validation").merge("event" => "validation_failed"),
      operational_event("field_qa", result: "fail").merge("event" => "field_qa_passed")
    ]
    cases.each { |event| assert_raises(Hid::ValidationError) { build(event) } }
  end

  def test_unknown_type_domain_or_schema_fails_closed
    %w[unknown_event validation_started].each do |name|
      event = operational_event("local_validation").merge("event" => name)
      assert_raises(Hid::ValidationError) { build(event) }
    end
    event = operational_event("final_review").merge("schema_version" => "0.0.99")
    assert_raises(Hid::ValidationError) { build(event) }
    event = operational_event("final_review").merge("control_record" => {"plane" => "subject_baseline"})
    assert_raises(Hid::ValidationError) { build(event) }
  end

  def test_all_legacy_baseline_events_remain_non_authoritative
    events = File.readlines(File.expand_path("../events.jsonl", __dir__)).map { |line| JSON.parse(line) }
    assert_equal 59, events.length
    events.each { |event| assert_nil build(event) }
    legacy = human_event(decision: "denied").merge("schema_version" => "0.0.2")
    legacy["outcome"] = {"gate" => "final_review", "result" => "pass"}
    assert_nil build(legacy)
  end

  def test_metadata_is_not_authority
    event = operational_event("final_review")
    event["metadata"] = {"domain" => "HUMAN_DECISION", "mandated" => true}
    assert_equal "REVIEW_VERDICT", build(event).domain
    event["outcome"].delete("mandate")
    assert_raises(Hid::ValidationError) { build(event) }
  end

  def test_evidence_cannot_be_missing_stale_or_git_inconsistent
    missing = operational_event("local_validation")
    missing["outcome"].delete("evidence")
    stale = operational_event("local_validation")
    stale["outcome"]["evidence"]["artifact"]["tree"] = OTHER_TREE
    false_git = operational_event("local_validation")
    false_git["artifact"]["tree"] = OTHER_TREE
    false_git["outcome"]["evidence"]["artifact"]["tree"] = OTHER_TREE
    [missing, stale, false_git].each { |event| assert_raises(Hid::ValidationError) { build(event) } }
  end

  def test_initial_architecture_is_policy_mandated_development_authorization
    fact = build(operational_event("architecture"))
    assert_equal "REVIEW_VERDICT", fact.domain
    assert_equal "development_authorization", fact.phase
    assert_equal "iamine-architecture", fact.authority["mandate"]
    human = human_event(decision: "denied")
    human["authorization"]["gate"] = "architecture"
    assert_raises(Hid::ValidationError) { build(human) }
  end
end
