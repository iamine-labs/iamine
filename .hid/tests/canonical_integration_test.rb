# frozen_string_literal: true

require "open3"
require "tmpdir"
require_relative "test_helper"

class HidCanonicalIntegrationTest < HidTestCase
  def test_linear_side_branch_descendant_cannot_self_declare_as_merge
    with_repository do |root|
      candidate = create_candidate(root)
      git!(root, "checkout", "-q", "-b", "side-result")
      commit_fixture(root, "side.txt", "side change", "linear side result")
      integration = git_artifact(root)
      git!(root, "checkout", "-q", candidate.fetch(:candidate_head))
      fixture = candidate.merge(root: root, integration: integration)

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [merged_event(fixture)])
      end

      assert_includes error.message, "CANDIDATE_INTEGRATION_MISMATCH"
    end
  end

  def test_side_branch_merge_is_not_canonical_integration
    with_side_branch_merge do |fixture|
      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [merged_event(fixture)])
      end

      assert_includes error.message, "CANONICAL_INTEGRATION_MISSING"
      assert_equal "canonical_integration_missing", derive_real_next_action(fixture, [merged_event(fixture)])
    end
  end

  def test_canonical_no_ff_merge_is_accepted
    with_canonical_merge do |fixture|
      validate_real_state(fixture, "MERGED", [merged_event(fixture)])
    end
  end

  def test_missing_canonical_ref_fails_closed
    with_side_branch_merge do |fixture|
      git!(fixture.fetch(:root), "branch", "-D", "develop")

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [merged_event(fixture)])
      end

      assert_includes error.message, "CANONICAL_REF_UNAVAILABLE"
      assert_equal "canonical_integration_not_verifiable", derive_real_next_action(fixture, [merged_event(fixture)])
    end
  end

  def test_event_target_cannot_override_canonical_target
    with_side_branch_merge do |fixture|
      event = merged_event(fixture, target_branch: "side-result")

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [event])
      end

      assert_includes error.message, "CANONICAL_TARGET_MISMATCH"
    end
  end

  def test_post_merge_validation_rejects_side_branch_result
    with_side_branch_merge do |fixture|
      events = [merged_event(fixture), workflow_event_for(fixture, "post_merge_validation_passed")]

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "POST-MERGE VALIDATION", events)
      end

      assert_includes error.message, "CANONICAL_INTEGRATION_MISSING"
      refute_equal "close_feature", derive_real_next_action(fixture, events, state: "POST-MERGE VALIDATION")
    end
  end

  def test_closure_rejects_side_branch_result
    with_side_branch_merge do |fixture|
      events = [
        merged_event(fixture),
        workflow_event_for(fixture, "post_merge_validation_passed"),
        workflow_event_for(fixture, "feature_closed")
      ]

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED / VALIDATED / CLOSED", events)
      end

      assert_includes error.message, "CANONICAL_INTEGRATION_MISSING"
    end
  end

  def test_additional_commit_after_candidate_is_not_authorized_integration
    with_repository do |root|
      candidate = create_candidate(root)
      commit_fixture(root, "extra.txt", "unauthorized", "extra change")
      git!(root, "checkout", "-q", "develop")
      git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "merge changed candidate")
      integration = git_artifact(root)
      git!(root, "checkout", "-q", candidate.fetch(:candidate_head))
      fixture = candidate.merge(root: root, integration: integration)

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [merged_event(fixture)])
      end

      assert_includes error.message, "CANDIDATE_INTEGRATION_MISMATCH"
    end
  end

  def test_unsupported_strategy_fails_closed
    with_canonical_merge do |fixture|
      event = merged_event(fixture, strategy: "fast_forward")

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [event])
      end

      assert_includes error.message, "INTEGRATION_STRATEGY_UNSUPPORTED"
    end
  end

  def test_event_source_must_match_current_authorized_candidate
    with_canonical_merge do |fixture|
      event = merged_event(fixture)
      event.fetch("integration")["source_head_sha"] = "f" * 40

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [event])
      end

      assert_includes error.message, "CANDIDATE_INTEGRATION_MISMATCH"
    end
  end

  private

  def with_repository
    Dir.mktmpdir("hid-canonical-integration") do |root|
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      commit_fixture(root, "base.txt", "base", "base")
      git!(root, "branch", "-M", "develop")
      yield root
    end
  end

  def with_side_branch_merge
    with_repository do |root|
      candidate = create_candidate(root)
      git!(root, "checkout", "-q", "-b", "side-result", "develop")
      git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "side merge")
      integration = git_artifact(root)
      git!(root, "checkout", "-q", "feature/test")
      yield candidate.merge(root: root, integration: integration)
    end
  end

  def with_canonical_merge
    with_repository do |root|
      candidate = create_candidate(root)
      git!(root, "checkout", "-q", "develop")
      git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "canonical merge")
      integration = git_artifact(root)
      git!(root, "checkout", "-q", "feature/test")
      yield candidate.merge(root: root, integration: integration)
    end
  end

  def create_candidate(root)
    git!(root, "checkout", "-q", "-b", "feature/test", "develop")
    commit_fixture(root, "candidate.txt", "candidate", "candidate")
    artifact = git_artifact(root)
    {candidate_head: artifact.fetch(:head), candidate_tree: artifact.fetch(:tree)}
  end

  def commit_fixture(root, path, content, message)
    File.write(File.join(root, path), content)
    git!(root, "add", path)
    git!(root, "commit", "-q", "-m", message)
  end

  def git_artifact(root)
    {
      head: git!(root, "rev-parse", "HEAD").strip,
      tree: git!(root, "rev-parse", "HEAD^{tree}").strip
    }
  end

  def merged_event(fixture, target_branch: "develop", strategy: "no_ff_merge")
    workflow_event(
      "merged",
      head: fixture.dig(:integration, :head),
      tree: fixture.dig(:integration, :tree),
      source_head: fixture.fetch(:candidate_head),
      source_tree: fixture.fetch(:candidate_tree),
      target_branch: target_branch,
      strategy: strategy
    )
  end

  def workflow_event_for(fixture, name)
    workflow_event(name, head: fixture.dig(:integration, :head), tree: fixture.dig(:integration, :tree))
  end

  def validate_real_state(fixture, state, lifecycle_events)
    validator = Hid::Validator.new(fixture.fetch(:root))
    current = current_candidate(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    feature = real_feature(state, fixture)
    events = authorization_events(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    evidence = {
      "HID-EVID-0001" => {
        "derived_status" => "VALID",
        "artifact" => {"head_sha" => fixture.fetch(:candidate_head), "tree" => fixture.fetch(:candidate_tree)}
      }
    }

    validate_state(feature, events + lifecycle_events, current: current, validator: validator, evidence: evidence)
  end

  def derive_real_next_action(fixture, lifecycle_events, state: "MERGED")
    validator = Hid::Validator.new(fixture.fetch(:root))
    current = current_candidate(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    events = authorization_events(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    validator.send(:derive_next_action, real_feature(state, fixture), events + lifecycle_events, state_project, current)
  end

  def real_feature(state, fixture)
    feature = state_feature(state)
    feature["git"]["candidate_snapshot"] = {
      "head_sha" => fixture.fetch(:candidate_head),
      "tree" => fixture.fetch(:candidate_tree)
    }
    feature
  end

  def git!(root, *args)
    stdout, stderr, status = Open3.capture3("git", "-C", root, *args)
    raise "git #{args.join(' ')} failed: #{stderr}" unless status.success?

    stdout
  end
end
