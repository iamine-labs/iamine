# frozen_string_literal: true

require_relative "test_helper"

class HidAppendOnlyGitTest < HidTestCase
  def test_append_only_accepts_preserved_baseline_prefix
    result = @validator.validate_append_only_prefix("A\nB\nC\n", "A\nB\nC\nD\n", "fixture")
    assert_equal "baseline_prefix_preserved", result
  end

  def test_append_only_rejects_baseline_mutation
    assert_raises(Hid::ValidationError) do
      @validator.validate_append_only_prefix("A\nB\nC\n", "A\nX\nC\nD\n", "fixture")
    end
  end

  def test_missing_local_origin_develop_is_not_reported_as_pass
    validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new)
    path = File.expand_path("../events.jsonl", __dir__)

    assert_equal "not_checked", validator.send(:validate_append_only, path)
    assert_includes validator.warnings, "append_only=not_checked reason=git_base_unavailable"
  end

  def test_git_capture_derives_identity_from_local_tracking_ref
    root = File.expand_path("../..", __dir__)
    capture = Hid::GitFacts.new(root).capture

    assert_equal "feature/hid-shadow-mode-001", capture["branch"]
    assert_match(/\A[0-9a-f]{40}\z/, capture["head_sha"])
    assert_match(/\A[0-9a-f]{40}\z/, capture["tree"])
    assert_equal "refs/remotes/origin/develop", capture["base_ref"]
    assert_equal "local_tracking_ref", capture["base_ref_scope"]
    assert_equal "not_verified", capture["base_ref_freshness"]
    assert_includes %w[base_is_ancestor diverged unknown], capture["ancestry"]
  end
end
