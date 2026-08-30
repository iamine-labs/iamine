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
