# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/hid/validator"

class FakeGit
  def initialize(trees = {})
    @trees = trees
  end

  def commit_tree(sha)
    @trees.fetch(sha, [:invalid, nil])
  end
end

class HidValidatorTest < Minitest::Test
  HEAD = "a" * 40
  TREE = "b" * 40
  OTHER_HEAD = "c" * 40
  OTHER_TREE = "d" * 40

  def setup
    @project = {
      "human_gates" => {
        "human_merge" => {"action" => "merge", "artifact_bound" => true}
      }
    }
    @feature = {
      "id" => "HID-SHADOW-MODE-001",
      "git" => {"candidate_snapshot" => {"head_sha" => HEAD, "tree" => TREE}},
      "gates" => {"human_merge" => {"status" => "passed"}},
      "evidence" => ["HID-EVID-0001"]
    }
    @validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new(HEAD => [:valid, TREE]))
  end

  def test_passed_human_gate_without_event_fails
    assert_raises(Hid::ValidationError) do
      @validator.validate_human_gates(@feature, [], @project)
    end
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

    assert_raises(Hid::ValidationError) do
      @validator.validate_human_gates(@feature, [event], @project)
    end
  end

  def test_matching_human_authorization_supports_gate
    @validator.validate_human_authorization_event(authorization_event, @project, "fixture")
    @validator.validate_human_gates(@feature, [authorization_event], @project)
  end

  def test_authorization_action_must_match_gate
    event = authorization_event
    event["authorization"]["action"] = "release"

    assert_raises(Hid::ValidationError) do
      @validator.validate_human_authorization_event(event, @project, "fixture")
    end
  end

  def test_evidence_for_current_clean_artifact_is_valid
    evidence = evidence_record(HEAD, TREE)
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}

    assert_equal "VALID", @validator.classify_evidence(evidence, current)
  end

  def test_evidence_for_another_current_artifact_is_stale
    evidence = evidence_record(HEAD, TREE)
    current = {"head_sha" => OTHER_HEAD, "tree" => OTHER_TREE, "dirty" => false}

    assert_equal "STALE", @validator.classify_evidence(evidence, current)
  end

  def test_commit_tree_contradiction_is_invalid
    evidence = evidence_record(HEAD, OTHER_TREE)
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}

    assert_equal "INVALID", @validator.classify_evidence(evidence, current)
  end

  def test_unverifiable_commit_is_unknown
    validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new(HEAD => [:unknown, nil]))
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}

    assert_equal "UNKNOWN", validator.classify_evidence(evidence_record(HEAD, TREE), current)
  end

  def test_missing_referenced_evidence_fails
    assert_raises(Hid::ValidationError) do
      @validator.validate_evidence_references(@feature, {})
    end
  end

  def test_api_key_field_is_privacy_violation
    policy = Hid::PrivacyPolicy.load(File.expand_path("../privacy.yaml", __dir__))
    findings = policy.findings({"api_key" => "value"}, "fixture")

    assert findings.any? { |finding| finding.level == "privacy_violation" }
  end

  def test_local_path_is_privacy_warning
    policy = Hid::PrivacyPolicy.load(File.expand_path("../privacy.yaml", __dir__))
    findings = policy.findings({"note" => "/Users/person/project"}, "fixture")

    assert findings.any? { |finding| finding.level == "privacy_warning" }
  end

  def test_git_capture_derives_current_identity
    root = File.expand_path("../..", __dir__)
    capture = Hid::GitFacts.new(root).capture

    assert_equal "feature/hid-shadow-mode-001", capture["branch"]
    assert_match(/\A[0-9a-f]{40}\z/, capture["head_sha"])
    assert_match(/\A[0-9a-f]{40}\z/, capture["tree"])
    assert_includes %w[base_is_ancestor diverged unknown], capture["ancestry"]
  end

  private

  def authorization_event
    {
      "event" => "human_authorization",
      "feature" => "HID-SHADOW-MODE-001",
      "actor" => {"type" => "human", "role" => "merge-owner"},
      "authorization" => {"gate" => "human_merge", "action" => "merge", "decision" => "approved"},
      "artifact" => {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}
    }
  end

  def evidence_record(head, tree)
    {"artifact" => {"head_sha" => head, "tree" => tree}}
  end
end
