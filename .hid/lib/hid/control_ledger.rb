# frozen_string_literal: true

require "open3"

module Hid
  class ControlLedgerError < StandardError; end
  class ControlLedgerChanged < ControlLedgerError; end

  class ControlLedger
    Entry = Struct.new(:json, :ledger_commit, keyword_init: true)

    SHA_PATTERN = /\A[0-9a-f]{40}\z/
    REF_PATTERN = %r{\Arefs/heads/hid/[A-Za-z0-9][A-Za-z0-9._/-]*\z}
    PATH_PATTERN = /\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
    ZERO_SHA = "0" * 40
    CURRENT_HEAD = Object.new.freeze

    attr_reader :ref, :events_path

    def initialize(root, ref:, events_path:)
      raise ControlLedgerError, "invalid control ledger ref" unless REF_PATTERN.match?(ref.to_s)
      raise ControlLedgerError, "invalid control ledger events path" unless PATH_PATTERN.match?(events_path.to_s)

      @root = root
      @ref = ref
      @events_path = events_path
    end

    def head
      output, status = run("rev-parse", "--verify", "#{ref}^{commit}")
      return output.strip if status.success? && SHA_PATTERN.match?(output.strip)
      return nil if status.exitstatus == 128

      raise ControlLedgerError, "control ledger ref is not verifiable"
    end

    def entries
      tip = head
      return [] if tip.nil?

      commits = required("rev-list", "--reverse", "--first-parent", tip).lines.map(&:strip).reject(&:empty?)
      previous_commit = nil
      previous_content = ""

      commits.map do |commit|
        parents = required("rev-list", "--parents", "-n", "1", commit).split.drop(1)
        expected_parents = previous_commit.nil? ? [] : [previous_commit]
        raise ControlLedgerError, "control ledger history is not linear" unless parents == expected_parents

        paths = required("ls-tree", "-r", "--name-only", commit).lines.map(&:strip).reject(&:empty?)
        raise ControlLedgerError, "control ledger commit contains unexpected paths" unless paths == [events_path]

        content = required("show", "#{commit}:#{events_path}")
        previous_lines = event_lines(previous_content)
        current_lines = event_lines(content)
        unless current_lines.first(previous_lines.length) == previous_lines && current_lines.length == previous_lines.length + 1
          raise ControlLedgerError, "control ledger commit must append exactly one event"
        end

        previous_commit = commit
        previous_content = content
        Entry.new(json: current_lines.last, ledger_commit: commit)
      end
    end

    def append(event_json, expected_head: CURRENT_HEAD)
      line = normalize_event(event_json)
      observed_head = head
      expected = expected_head.equal?(CURRENT_HEAD) ? observed_head : expected_head
      raise ControlLedgerChanged, "CONTROL_LEDGER_CHANGED" unless expected == observed_head

      previous_content = observed_head ? required("show", "#{observed_head}:#{events_path}") : ""
      content = previous_content + line + "\n"
      blob = required_with_input(content, "hash-object", "-w", "--stdin").strip
      tree_line = "100644 blob #{blob}\t#{events_path}\n"
      tree = required_with_input(tree_line, "mktree").strip
      commit_args = ["commit-tree", tree]
      commit_args.concat(["-p", observed_head]) if observed_head
      commit_args.concat(["-F", "-"])
      commit = required_with_input("HID control event\n", *commit_args).strip

      old_value = observed_head || ZERO_SHA
      _output, status = run("update-ref", ref, commit, old_value)
      raise ControlLedgerChanged, "CONTROL_LEDGER_CHANGED" unless status.success?

      commit
    end

    def containment_status(branch)
      ledger_head = head
      return :absent if ledger_head.nil?

      branch_head, branch_status = run("rev-parse", "--verify", "refs/heads/#{branch}^{commit}")
      return :unavailable unless branch_status.success?

      commits = required("rev-list", ledger_head).lines.map(&:strip).reject(&:empty?)
      commits.each do |commit|
        _output, status = run("merge-base", "--is-ancestor", commit, branch_head.strip)
        return :contained if status.success?
        return :unknown unless status.exitstatus == 1
      end

      :not_contained
    end

    private

    def normalize_event(event_json)
      line = event_json.to_s
      line = line.delete_suffix("\n")
      raise ControlLedgerError, "control event must be one non-empty JSONL line" if line.empty? || line.include?("\n") || line.include?("\r")

      line
    end

    def event_lines(content)
      return [] if content.empty?
      raise ControlLedgerError, "control ledger JSONL must end with a newline" unless content.end_with?("\n")

      content.lines(chomp: true)
    end

    def required(*args)
      output, status = run(*args)
      raise ControlLedgerError, "git #{args.join(' ')} failed" unless status.success?

      output
    end

    def required_with_input(input, *args)
      stdout, _stderr, status = Open3.capture3("git", "-C", @root, *args, stdin_data: input)
      raise ControlLedgerError, "git #{args.join(' ')} failed" unless status.success?

      stdout
    rescue Errno::ENOENT => e
      raise ControlLedgerError, e.message
    end

    def run(*args)
      stdout, _stderr, status = Open3.capture3("git", "-C", @root, *args)
      [stdout, status]
    rescue Errno::ENOENT => e
      raise ControlLedgerError, e.message
    end
  end
end
