# frozen_string_literal: true

require_relative "operational_test_helper"

class HidOperationalLifecycleTest < HidTestCase
  include OperationalFixture

  def test_final_review_after_merge_cannot_repair_transition
    events = approved_events
    final = events.delete_at(4)
    events += [merge_fact, final]
    assert_inconsistent(events)
  end

  def test_closure_without_post_merge_is_rejected
    assert_inconsistent(approved_events + [merge_fact, lifecycle_fact("closure")])
  end

  def test_post_merge_before_merge_is_rejected
    assert_inconsistent(approved_events + [lifecycle_fact("post_merge_validation"), merge_fact])
  end

  def test_denial_before_merge_blocks_and_after_merge_preserves_history
    denial = human_event(decision: "denied")
    assert_inconsistent(approved_events + [denial, merge_fact])
    result = project_lifecycle(approved_events + [merge_fact, human_event(decision: "denied")])
    assert_equal "MERGED", result["state"]
    assert_empty result["failures"]
  end

  def test_failed_post_merge_validation_blocks_closure_until_new_pass
    events = approved_events + [merge_fact, lifecycle_fact("post_merge_validation")]
    failed = lifecycle_fact("post_merge_validation", result: "fail")
    Hid::OperationalFacts.new(state_project).validate!(failed)
    events << failed
    assert_equal "CHANGES REQUIRED", project_lifecycle(events)["state"]
    assert_inconsistent(events + [lifecycle_fact("closure")])
    events += [lifecycle_fact("post_merge_validation"), lifecycle_fact("closure")]
    assert_equal "MERGED / VALIDATED / CLOSED", project_lifecycle(events)["state"]
  end

  def test_wrong_post_merge_artifact_cannot_support_closure
    wrong = operational_event("post_merge_validation")
    assert_inconsistent(approved_events + [merge_fact, wrong, lifecycle_fact("closure")])
  end

  def test_deterministic_merge_tree_and_canonical_containment_still_required
    events = approved_events + [merge_fact]
    mismatch = project_lifecycle(events, relation: :merge_tree_mismatch)
    assert mismatch["failures"].any? { |error| error.start_with?("MERGE_TREE_MISMATCH") }
    missing = project_lifecycle(events, containment: :not_contained)
    assert missing["failures"].any? { |error| error.start_with?("CANONICAL_INTEGRATION_MISSING") }
  end

  def test_unresolved_additional_policy_event_requirement_fails_closed
    project = Marshal.load(Marshal.dump(state_project))
    project["state_requirements"]["APPROVED FOR MERGE"]["events"] = ["field_qa_passed"]
    result = operational_projection(nonhuman_events, project: project)
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
    assert result["failures"].any? { |error| error.start_with?("POLICY_INCOMPLETE") }
  end

  private

  def lifecycle_fact(gate, result: "pass")
    operational_event(gate, result: result, artifact: current_candidate(head: OTHER_HEAD, tree: OTHER_TREE))
  end

  def merge_fact
    lifecycle_fact("merged").merge("integration" => {
      "source_head_sha" => HEAD, "source_tree" => TREE, "target_branch" => "develop", "strategy" => "no_ff_merge"
    })
  end

  def project_lifecycle(events, relation: :valid, containment: :contained)
    git = FakeGit.new(
      {HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE]}, current: current_candidate,
      ancestries: {[HEAD, OTHER_HEAD] => :ancestor},
      relations: {[HEAD, OTHER_HEAD, "no_ff_merge"] => relation},
      containments: {[OTHER_HEAD, "develop"] => containment}
    )
    operational_projection(events, git: git)
  end

  def assert_inconsistent(events)
    result = project_lifecycle(events)
    assert_equal "CHANGES REQUIRED", result["state"]
    refute_empty result["failures"]
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
  end
end
