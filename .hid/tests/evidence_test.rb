# frozen_string_literal: true

require_relative "test_helper"

class HidEvidenceTest < HidTestCase
  def test_evidence_for_current_clean_artifact_is_valid
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}
    assert_equal "VALID", @validator.classify_evidence(evidence_record(HEAD, TREE), current)
  end

  def test_evidence_for_another_current_artifact_is_stale
    current = {"head_sha" => OTHER_HEAD, "tree" => OTHER_TREE, "dirty" => false}
    assert_equal "STALE", @validator.classify_evidence(evidence_record(HEAD, TREE), current)
  end

  def test_commit_tree_contradiction_is_invalid
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}
    assert_equal "INVALID", @validator.classify_evidence(evidence_record(HEAD, OTHER_TREE), current)
  end

  def test_unverifiable_commit_is_unknown
    validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new(HEAD => [:unknown, nil]))
    current = {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}

    assert_equal "UNKNOWN", validator.classify_evidence(evidence_record(HEAD, TREE), current)
  end

  def test_missing_referenced_evidence_fails
    assert_raises(Hid::ValidationError) { @validator.validate_evidence_references(@feature, {}) }
  end
end
