# frozen_string_literal: true

require "open3"

module Hid
  class GitUnavailable < StandardError; end

  class GitFacts
    SHA_PATTERN = /\A[0-9a-f]{40}\z/

    def initialize(root)
      @root = root
    end

    def capture
      head = required("rev-parse", "HEAD").strip
      base = optional("rev-parse", "origin/develop")

      {
        "branch" => required("branch", "--show-current").strip,
        "head_sha" => head,
        "tree" => required("rev-parse", "HEAD^{tree}").strip,
        "dirty" => !required("status", "--porcelain=v1").empty?,
        "base_ref" => "refs/remotes/origin/develop",
        "base_ref_scope" => "local_tracking_ref",
        "base_ref_freshness" => "not_verified",
        "base_sha" => base&.strip,
        "ancestry" => ancestry(base&.strip, head)
      }
    end

    def baseline_file(path)
      base = optional("rev-parse", "origin/develop")
      return [:unavailable, nil] if base.nil? || base.strip.empty?

      content, status = run("show", "#{base.strip}:#{path}")
      status.success? ? [:available, content] : [:missing, nil]
    rescue GitUnavailable
      [:unavailable, nil]
    end

    def commit_tree(sha)
      return [:invalid, nil] unless SHA_PATTERN.match?(sha.to_s)

      commit, commit_status = run("rev-parse", "--verify", "#{sha}^{commit}")
      return [:invalid, nil] unless commit_status.success?

      tree, tree_status = run("rev-parse", "--verify", "#{commit.strip}^{tree}")
      return [:unknown, nil] unless tree_status.success?

      [:valid, tree.strip]
    rescue GitUnavailable
      [:unknown, nil]
    end

    def artifact_status(head_sha, expected_tree)
      state, actual_tree = commit_tree(head_sha)
      return state unless state == :valid

      actual_tree == expected_tree ? :valid : :invalid
    end

    def ancestry_status(ancestor_sha, descendant_sha)
      return :invalid unless SHA_PATTERN.match?(ancestor_sha.to_s) && SHA_PATTERN.match?(descendant_sha.to_s)

      _output, status = run("merge-base", "--is-ancestor", ancestor_sha, descendant_sha)
      return :ancestor if status.success?
      return :unrelated if status.exitstatus == 1

      :unknown
    rescue GitUnavailable
      :unknown
    end

    def canonical_integration_status(commit_sha, branch)
      return :invalid unless SHA_PATTERN.match?(commit_sha.to_s) && branch.is_a?(String) && !branch.empty?

      target, target_status = run("rev-parse", "--verify", "refs/heads/#{branch}^{commit}")
      return :unavailable unless target_status.success?

      _output, status = run("merge-base", "--is-ancestor", commit_sha, target.strip)
      return :contained if status.success?
      return :not_contained if status.exitstatus == 1

      :unknown
    rescue GitUnavailable
      :unknown
    end

    def merge_relation_status(candidate_sha, integration_sha, strategy)
      return :unsupported unless strategy == "no_ff_merge"
      return :invalid unless SHA_PATTERN.match?(candidate_sha.to_s) && SHA_PATTERN.match?(integration_sha.to_s)

      output, status = run("rev-list", "--parents", "-n", "1", integration_sha)
      return :unknown unless status.success?

      parents = output.split.drop(1)
      return :invalid unless parents.length == 2 && parents.last == candidate_sha && parents.first != candidate_sha

      artifact_status, actual_tree = commit_tree(integration_sha)
      return :unknown if artifact_status == :unknown
      return :invalid unless artifact_status == :valid

      merge_status, expected_tree = expected_merge_tree(parents.first, candidate_sha)
      return :merge_not_clean if merge_status == :conflict
      return :merge_tree_not_verifiable unless merge_status == :clean

      actual_tree == expected_tree ? :valid : :merge_tree_mismatch
    rescue GitUnavailable
      :unknown
    end

    def expected_merge_tree(parent1_sha, candidate_sha)
      return [:invalid, nil] unless SHA_PATTERN.match?(parent1_sha.to_s) && SHA_PATTERN.match?(candidate_sha.to_s)

      output, status = run("merge-tree", "--write-tree", parent1_sha, candidate_sha)
      return [:conflict, nil] if status.exitstatus == 1
      return [:unknown, nil] unless status.success?

      tree = output.lines.first&.strip
      return [:unknown, nil] unless SHA_PATTERN.match?(tree.to_s)

      verified, tree_status = run("rev-parse", "--verify", "#{tree}^{tree}")
      return [:unknown, nil] unless tree_status.success? && verified.strip == tree

      [:clean, tree]
    rescue GitUnavailable
      [:unknown, nil]
    end

    private

    def ancestry(base, head)
      return "unknown" if base.nil? || base.empty?

      _output, status = run("merge-base", "--is-ancestor", base, head)
      return "base_is_ancestor" if status.success?
      return "diverged" if status.exitstatus == 1

      "unknown"
    end

    def required(*args)
      output, status = run(*args)
      raise GitUnavailable, "git #{args.join(' ')} failed" unless status.success?

      output
    end

    def optional(*args)
      output, status = run(*args)
      status.success? ? output : nil
    end

    def run(*args)
      stdout, _stderr, status = Open3.capture3("git", "-C", @root, *args)
      [stdout, status]
    rescue Errno::ENOENT => e
      raise GitUnavailable, e.message
    end
  end
end
