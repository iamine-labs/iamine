# frozen_string_literal: true

require "open3"
require "tmpdir"
require_relative "test_helper"

class HidMergeTreeIntegrityTest < HidTestCase
  def test_actual_no_ff_merge_matches_expected_tree
    with_clean_context do |fixture|
      integration = create_actual_merge(fixture)

      assert_equal fixture.fetch(:expected_tree), integration.fetch(:tree)
      validate_real_state(fixture.merge(integration: integration), "MERGED", [merged_event(fixture, integration)])
    end
  end

  def test_manual_topology_with_legitimate_tree_is_accepted
    with_clean_context do |fixture|
      integration = manual_integration(fixture, fixture.fetch(:expected_tree), "manual legitimate merge")

      validate_real_state(fixture.merge(integration: integration), "MERGED", [merged_event(fixture, integration)])
    end
  end

  def test_added_file_attack_is_rejected
    assert_tree_attack_rejected(:added)
  end

  def test_modified_file_attack_is_rejected
    assert_tree_attack_rejected(:modified)
  end

  def test_deleted_file_attack_is_rejected
    assert_tree_attack_rejected(:deleted)
  end

  def test_merge_conflict_is_detected
    with_conflict_context do |fixture|
      status, tree = Hid::GitFacts.new(fixture.fetch(:root)).expected_merge_tree(
        fixture.fetch(:parent1_head),
        fixture.fetch(:candidate_head)
      )

      assert_equal :conflict, status
      assert_nil tree
    end
  end

  def test_manually_resolved_conflict_cannot_satisfy_merge
    with_conflict_context do |fixture|
      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", [merged_event(fixture, fixture.fetch(:integration))])
      end

      assert_includes error.message, "MERGE_NOT_CLEAN"
      assert_equal "merge_not_clean", derive_real_next_action(fixture, [merged_event(fixture, fixture.fetch(:integration))])
    end
  end

  def test_invalid_merge_tree_cannot_support_post_merge_or_closure
    with_tree_attack(:added) do |fixture|
      events = [
        merged_event(fixture, fixture.fetch(:integration)),
        workflow_event_for(fixture, "post_merge_validation_passed"),
        workflow_event_for(fixture, "feature_closed")
      ]

      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED / VALIDATED / CLOSED", events)
      end

      assert_includes error.message, "MERGE_TREE_MISMATCH"
    end
  end

  def test_unverifiable_merge_tree_fails_closed
    relation_key = [HEAD, OTHER_HEAD, "no_ff_merge"]
    git = FakeGit.new(
      {HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE]},
      current: current_candidate,
      ancestries: {[HEAD, OTHER_HEAD] => :ancestor},
      relations: {relation_key => :merge_tree_not_verifiable}
    )
    validator = Hid::Validator.new(Dir.pwd, git: git)
    feature = state_feature("MERGED")
    events = authorization_events + [workflow_event("merged", head: OTHER_HEAD, tree: OTHER_TREE)]

    error = assert_raises(Hid::ValidationError) do
      validate_state(feature, events, validator: validator)
    end
    assert_includes error.message, "MERGE_TREE_NOT_VERIFIABLE"
    assert_equal(
      "merge_tree_not_verifiable",
      validator.send(:derive_next_action, feature, events, state_project, current_candidate)
    )
  end

  private

  def with_repository
    Dir.mktmpdir("hid-merge-tree-integrity") do |root|
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      commit_fixture(root, "shared.txt", "base", "base")
      git!(root, "branch", "-M", "develop")
      yield root
    end
  end

  def with_clean_context
    with_repository do |root|
      base_head = git!(root, "rev-parse", "HEAD").strip
      git!(root, "checkout", "-q", "-b", "feature/test")
      candidate = commit_fixture(root, "candidate.txt", "authorized", "candidate")
      git!(root, "checkout", "-q", "develop")
      parent1 = commit_fixture(root, "canonical.txt", "canonical", "canonical parent")
      status, expected_tree = Hid::GitFacts.new(root).expected_merge_tree(parent1.fetch(:head), candidate.fetch(:head))
      raise "expected clean fixture, got #{status}" unless status == :clean

      yield(
        root: root,
        base_head: base_head,
        candidate_head: candidate.fetch(:head),
        candidate_tree: candidate.fetch(:tree),
        parent1_head: parent1.fetch(:head),
        expected_tree: expected_tree
      )
    end
  end

  def with_tree_attack(kind)
    with_clean_context do |fixture|
      root = fixture.fetch(:root)
      create_actual_merge(fixture)
      case kind
      when :added
        File.write(File.join(root, "unauthorized.txt"), "unauthorized")
        git!(root, "add", "unauthorized.txt")
      when :modified
        File.write(File.join(root, "candidate.txt"), "tampered")
        git!(root, "add", "candidate.txt")
      when :deleted
        File.delete(File.join(root, "canonical.txt"))
        git!(root, "add", "-u", "canonical.txt")
      end
      git!(root, "commit", "-q", "-m", "build manipulated tree")
      manipulated_tree = git!(root, "rev-parse", "HEAD^{tree}").strip
      integration = manual_integration(fixture, manipulated_tree, "manual manipulated merge")
      yield fixture.merge(integration: integration)
    end
  end

  def with_conflict_context
    with_repository do |root|
      git!(root, "checkout", "-q", "-b", "feature/test")
      candidate = commit_fixture(root, "shared.txt", "candidate change", "candidate conflict")
      git!(root, "checkout", "-q", "develop")
      parent1 = commit_fixture(root, "shared.txt", "canonical change", "canonical conflict")

      File.write(File.join(root, "shared.txt"), "manual resolution")
      git!(root, "add", "shared.txt")
      resolved_tree = git!(root, "write-tree").strip
      fixture = {
        root: root,
        candidate_head: candidate.fetch(:head),
        candidate_tree: candidate.fetch(:tree),
        parent1_head: parent1.fetch(:head)
      }
      integration = manual_integration(fixture, resolved_tree, "manual conflict resolution")
      yield fixture.merge(integration: integration)
    end
  end

  def create_actual_merge(fixture)
    root = fixture.fetch(:root)
    git!(root, "checkout", "-q", "develop")
    git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "canonical merge")
    git_artifact(root)
  end

  def manual_integration(fixture, tree, message)
    root = fixture.fetch(:root)
    head = git!(
      root,
      "commit-tree",
      tree,
      "-p",
      fixture.fetch(:parent1_head),
      "-p",
      fixture.fetch(:candidate_head),
      "-m",
      message
    ).strip
    git!(root, "update-ref", "refs/heads/develop", head)
    git!(root, "checkout", "-q", "feature/test")
    {head: head, tree: tree}
  end

  def commit_fixture(root, path, content, message)
    File.write(File.join(root, path), content)
    git!(root, "add", path)
    git!(root, "commit", "-q", "-m", message)
    git_artifact(root)
  end

  def git_artifact(root)
    {
      head: git!(root, "rev-parse", "HEAD").strip,
      tree: git!(root, "rev-parse", "HEAD^{tree}").strip
    }
  end

  def merged_event(fixture, integration)
    workflow_event(
      "merged",
      head: integration.fetch(:head),
      tree: integration.fetch(:tree),
      source_head: fixture.fetch(:candidate_head),
      source_tree: fixture.fetch(:candidate_tree)
    )
  end

  def workflow_event_for(fixture, name)
    integration = fixture.fetch(:integration)
    workflow_event(name, head: integration.fetch(:head), tree: integration.fetch(:tree))
  end

  def assert_tree_attack_rejected(kind)
    with_tree_attack(kind) do |fixture|
      events = [merged_event(fixture, fixture.fetch(:integration))]
      error = assert_raises(Hid::ValidationError) do
        validate_real_state(fixture, "MERGED", events)
      end

      assert_includes error.message, "MERGE_TREE_MISMATCH"
      assert_equal "merge_tree_mismatch", derive_real_next_action(fixture, events) if kind == :added
    end
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

  def derive_real_next_action(fixture, lifecycle_events)
    validator = Hid::Validator.new(fixture.fetch(:root))
    current = current_candidate(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    events = authorization_events(head: fixture.fetch(:candidate_head), tree: fixture.fetch(:candidate_tree))
    validator.send(:derive_next_action, real_feature("MERGED", fixture), events + lifecycle_events, state_project, current)
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
