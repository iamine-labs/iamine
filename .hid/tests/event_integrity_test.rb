# frozen_string_literal: true

require "open3"
require "tmpdir"
require_relative "test_helper"

class HidEventIntegrityTest < HidTestCase
  FAKE_HEAD = "f" * 40
  FAKE_TREE = "e" * 40

  def test_merged_state_rejects_nonexistent_git_artifact
    assert_invalid_event_artifact("MERGED", [workflow_event("merged", head: FAKE_HEAD, tree: FAKE_TREE)])
  end

  def test_post_merge_state_rejects_nonexistent_merge_artifact
    assert_invalid_event_artifact("POST-MERGE VALIDATION", [workflow_event("merged", head: FAKE_HEAD, tree: FAKE_TREE)])
  end

  def test_post_merge_state_requires_post_merge_validation_event
    events = authorization_events + [merged_event]

    error = assert_raises(Hid::ValidationError) do
      validate_state(state_feature("POST-MERGE VALIDATION"), events, validator: lifecycle_validator)
    end
    assert_includes error.message, "event post_merge_validation_passed is required"
  end

  def test_closed_state_rejects_nonexistent_lifecycle_artifacts
    events = %w[merged post_merge_validation_passed feature_closed].map do |name|
      workflow_event(name, head: FAKE_HEAD, tree: FAKE_TREE)
    end
    assert_invalid_event_artifact("MERGED / VALIDATED / CLOSED", events)
  end

  def test_merged_state_rejects_unrelated_real_artifact
    feature = state_feature("MERGED")
    git = FakeGit.new(
      {HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE]},
      current: current_candidate
    )
    validator = Hid::Validator.new(Dir.pwd, git: git)
    events = authorization_events + [workflow_event("merged", head: OTHER_HEAD, tree: OTHER_TREE)]

    error = assert_raises(Hid::ValidationError) do
      validate_state(feature, events, validator: validator)
    end
    assert_includes error.message, "INVALID_EVENT_ARTIFACT"
  end

  def test_current_candidate_commit_cannot_self_declare_as_merge_commit
    feature = state_feature("MERGED")
    events = authorization_events + [workflow_event("merged")]

    error = assert_raises(Hid::ValidationError) { validate_state(feature, events) }
    assert_includes error.message, "INVALID_EVENT_ARTIFACT"
  end

  def test_merged_state_rejects_real_commit_with_wrong_tree
    feature = state_feature("MERGED")
    events = authorization_events + [workflow_event("merged", head: OTHER_HEAD, tree: FAKE_TREE)]

    error = assert_raises(Hid::ValidationError) { validate_state(feature, events) }
    assert_includes error.message, "INVALID_EVENT_ARTIFACT"
  end

  def test_historical_snapshot_ancestry_cannot_replace_current_candidate_ancestry
    feature = state_feature("MERGED")
    current = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    git = FakeGit.new(
      {HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE], FAKE_HEAD => [:valid, FAKE_TREE]},
      current: current,
      ancestries: {
        [HEAD, FAKE_HEAD] => :ancestor,
        [OTHER_HEAD, FAKE_HEAD] => :unrelated
      }
    )
    validator = Hid::Validator.new(Dir.pwd, git: git)
    events = authorization_events(head: OTHER_HEAD, tree: OTHER_TREE)
    events << workflow_event("merged", head: FAKE_HEAD, tree: FAKE_TREE)

    error = assert_raises(Hid::ValidationError) do
      validate_state(feature, events, current: current, validator: validator)
    end
    assert_includes error.message, "INVALID_EVENT_ARTIFACT"
  end

  def test_unverifiable_required_event_artifact_fails_closed
    feature = state_feature("MERGED")
    git = FakeGit.new(
      {HEAD => [:valid, TREE], FAKE_HEAD => [:unknown, nil]},
      current: current_candidate
    )
    validator = Hid::Validator.new(Dir.pwd, git: git)
    events = authorization_events + [workflow_event("merged", head: FAKE_HEAD, tree: FAKE_TREE)]

    error = assert_raises(Hid::ValidationError) do
      validate_state(feature, events, validator: validator)
    end
    assert_includes error.message, "EVENT_ARTIFACT_UNKNOWN"
  end

  def test_closed_state_accepts_real_git_artifact_and_ancestry
    Dir.mktmpdir("hid-event-integrity") do |root|
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      candidate_head, candidate_tree = commit_fixture(root, "candidate", "candidate")
      merged_head, merged_tree = commit_fixture(root, "merged", "merged")
      git!(root, "checkout", "-q", candidate_head)

      git = Hid::GitFacts.new(root)
      validator = Hid::Validator.new(root, git: git)
      current = git.capture
      feature = state_feature("MERGED / VALIDATED / CLOSED")
      feature["git"]["candidate_snapshot"] = {"head_sha" => candidate_head, "tree" => candidate_tree}
      events = authorization_events(head: candidate_head, tree: candidate_tree)
      events += %w[merged post_merge_validation_passed feature_closed].map do |name|
        workflow_event(name, head: merged_head, tree: merged_tree)
      end
      evidence = {
        "HID-EVID-0001" => {
          "derived_status" => "STALE",
          "artifact" => {"head_sha" => candidate_head, "tree" => candidate_tree}
        }
      }

      validator.validate_human_gates(feature, events, state_project, current)
      validate_state(feature, events, current: current, validator: validator, evidence: evidence)
    end
  end

  def test_only_canonical_lifecycle_permutation_is_accepted
    tokens = %w[authorize merged post_merge_validation_passed feature_closed]
    validator = lifecycle_validator
    feature = state_feature("MERGED / VALIDATED / CLOSED")

    tokens.permutation.each do |permutation|
      events = lifecycle_events(permutation)
      if permutation == tokens
        validate_state(feature, events, validator: validator)
      else
        error = assert_raises(Hid::ValidationError, permutation.join(" -> ")) do
          validate_state(feature, events, validator: validator)
        end
        assert_includes error.message, "LIFECYCLE_ORDER_VIOLATION"
      end
    end
  end

  def test_approval_then_denial_before_merge_is_rejected
    approvals = authorization_events
    denial = authorization_event(decision: "denied")
    events = approvals + [denial, merged_event]

    error = assert_raises(Hid::ValidationError) do
      validate_state(state_feature("MERGED"), events, validator: lifecycle_validator)
    end
    assert_includes error.message, "LIFECYCLE_ORDER_VIOLATION"
    assert_equal "lifecycle_inconsistency", derive_lifecycle_next_action(state_feature("MERGED"), events)
  end

  def test_denial_after_merge_does_not_erase_valid_historical_transition
    events = authorization_events + [merged_event, authorization_event(decision: "denied")]

    validate_state(state_feature("MERGED"), events, validator: lifecycle_validator)
  end

  def test_final_review_authorization_after_merge_is_rejected
    approvals = authorization_events.reject { |event| event.dig("authorization", "gate") == "final_review" }
    late_final_review = authorization_event(gate: "final_review", action: "architecture_merge_approval")
    events = approvals + [merged_event, late_final_review]

    error = assert_raises(Hid::ValidationError) do
      validate_state(state_feature("MERGED"), events, validator: lifecycle_validator)
    end
    assert_includes error.message, "LIFECYCLE_ORDER_VIOLATION"
  end

  def test_other_feature_events_do_not_affect_lifecycle_order
    unrelated = merged_event
    unrelated["feature"] = "OTHER-FEATURE-001"
    events = authorization_events + [unrelated, merged_event, post_merge_event, closed_event]

    validate_state(state_feature("MERGED / VALIDATED / CLOSED"), events, validator: lifecycle_validator)
  end

  private

  def lifecycle_validator
    git = FakeGit.new(
      {HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE]},
      current: current_candidate,
      ancestries: {[HEAD, OTHER_HEAD] => :ancestor}
    )
    Hid::Validator.new(Dir.pwd, git: git)
  end

  def lifecycle_events(tokens)
    tokens.flat_map do |token|
      case token
      when "authorize"
        authorization_events
      when "merged"
        [merged_event]
      when "post_merge_validation_passed"
        [post_merge_event]
      when "feature_closed"
        [closed_event]
      end
    end
  end

  def merged_event
    workflow_event("merged", head: OTHER_HEAD, tree: OTHER_TREE)
  end

  def post_merge_event
    workflow_event("post_merge_validation_passed", head: OTHER_HEAD, tree: OTHER_TREE)
  end

  def closed_event
    workflow_event("feature_closed", head: OTHER_HEAD, tree: OTHER_TREE)
  end

  def derive_lifecycle_next_action(feature, events)
    lifecycle_validator.send(:derive_next_action, feature, events, state_project, current_candidate)
  end

  def assert_invalid_event_artifact(state, lifecycle_events)
    feature = state_feature(state)
    events = authorization_events + lifecycle_events

    error = assert_raises(Hid::ValidationError) { validate_state(feature, events) }
    assert_includes error.message, "INVALID_EVENT_ARTIFACT"
  end

  def commit_fixture(root, content, message)
    File.write(File.join(root, "fixture.txt"), content)
    git!(root, "add", "fixture.txt")
    git!(root, "commit", "-q", "-m", message)
    [git!(root, "rev-parse", "HEAD").strip, git!(root, "rev-parse", "HEAD^{tree}").strip]
  end

  def git!(root, *args)
    stdout, stderr, status = Open3.capture3("git", "-C", root, *args)
    raise "git #{args.join(' ')} failed: #{stderr}" unless status.success?

    stdout
  end
end
