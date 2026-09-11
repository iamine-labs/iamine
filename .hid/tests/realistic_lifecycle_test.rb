# frozen_string_literal: true

require "tmpdir"
require "fileutils"
require_relative "operational_test_helper"

class HidRealisticLifecycleTest < HidTestCase
  include OperationalFixture

  def test_real_manifest_through_full_lifecycle_without_subject_mutation
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      initial = subject_identity(root)
      ledger = Hid::ControlLedger.new(root, ref: "refs/heads/hid/control-plane", events_path: "events.jsonl")
      assert_nil ledger.head
      assert_equal "request_architecture_review", projection(validator)["next_action"]
      events = []
      nonhuman_events(artifact: candidate).each do |event|
        validator.append_control_event(event)
        events << event
        assert_equal initial, subject_identity(root), event["outcome"]["gate"]
        assert_equal "passed", projection(validator).dig("gates", event["outcome"]["gate"], "status")
      end
      assert_equal "READY", projection(validator).dig("human_gate_eligibility", "status")
      capsule = capsule_event(events, artifact: candidate, git: git)
      validator.append_control_event(capsule)
      validator.append_control_event(human_event(capsule, artifact: candidate))
      assert_equal initial, subject_identity(root)
      assert_equal "APPROVED FOR MERGE", projection(validator)["state"]
      assert_equal 1, ledger.entries.count { |entry| JSON.parse(entry.json)["event"] == "human_authorization" }

      git!(root, "checkout", "-q", "develop")
      git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "fixture canonical integration")
      integration = git.capture
      git!(root, "checkout", "-q", "feature/test")
      merged = operational_event("merged", artifact: integration)
      merged["integration"] = {"source_head_sha" => candidate["head_sha"], "source_tree" => candidate["tree"],
                                "target_branch" => "develop", "strategy" => "no_ff_merge"}
      validator.append_control_event(merged)
      assert_equal "MERGED", projection(validator)["state"]
      validator.append_control_event(operational_event("post_merge_validation", artifact: integration))
      assert_equal "POST-MERGE VALIDATION", projection(validator)["state"]
      validator.append_control_event(operational_event("closure", artifact: integration))
      closed = projection(validator)
      assert_equal "MERGED / VALIDATED / CLOSED", closed["state"]
      assert_equal "none", closed["next_action"]
      assert_empty closed["failures"]
      assert_equal candidate["head_sha"], git.capture["head_sha"]
      assert_equal candidate["tree"], git.capture["tree"]
      assert_equal :contained, git.canonical_integration_status(integration["head_sha"], "develop")
      assert_equal :valid, git.merge_relation_status(candidate["head_sha"], integration["head_sha"], "no_ff_merge")
      assert_equal 10, ledger.entries.length
    end
  end

  def test_supported_writer_rejects_missing_gates_and_bad_facts_before_write
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      ledger = Hid::ControlLedger.new(root, ref: "refs/heads/hid/control-plane", events_path: "events.jsonl")
      initial = subject_identity(root)
      premature = capsule_event([], artifact: candidate, git: git)
      bad_role = operational_event("architecture", artifact: candidate)
      bad_role["actor"]["role"] = "developer"
      bad_evidence = operational_event("local_validation", artifact: candidate)
      bad_evidence["outcome"]["evidence"]["artifact"]["tree"] = TREE
      false_tree = operational_event("architecture", artifact: candidate)
      false_tree["artifact"]["tree"] = TREE
      false_tree["outcome"]["evidence"]["artifact"]["tree"] = TREE
      private_event = operational_event("architecture", artifact: candidate)
      private_event["metadata"] = {"prompt" => "never-store"}
      legacy = human_event(premature, artifact: candidate).merge("schema_version" => "0.0.2")
      cases = [
        [premature, /APPROVAL_CAPSULE_NOT_READY/],
        [human_event(premature, artifact: candidate), /APPROVAL_CAPSULE_NOT_READY/],
        [bad_role, /OPERATIONAL_FACT_INVALID actor not authorized by mandate/],
        [bad_evidence, /OPERATIONAL_FACT_INVALID evidence feature\/artifact mismatch/],
        [false_tree, /OPERATIONAL_ARTIFACT_INVALID Git cannot verify the exact outcome artifact/],
        [private_event, /privacy_violation prohibited_payload .*metadata\.prompt/],
        [legacy, /new control events require operational schema 0\.0\.3/]
      ]
      cases.each do |event, reason|
        error = assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
        assert_match reason, error.message
        assert_nil ledger.head
        assert_equal initial, subject_identity(root)
      end
      orphan = operational_event("closure", artifact: candidate)
      error = assert_raises(Hid::ValidationError) { validator.append_control_event(orphan) }
      assert_equal "LIFECYCLE_ORDER_VIOLATION post-merge fact not applicable", error.message
      assert_nil ledger.head
    end
  end

  # The existing authority-domain matrix also uses this fixture. Pin its
  # rejection reasons here without changing the out-of-scope test file.
  def test_human_outcome_matrix_rejects_reserved_fields_for_the_expected_reason
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      initial = subject_identity(root)
      refs = git!(root, "for-each-ref", "--format=%(refname) %(objectname)")
      ledger = Hid::ControlLedger.new(root, ref: "refs/heads/hid/control-plane", events_path: "events.jsonl")
      assert_equal "request_architecture_review", projection(validator)["next_action"]
      %w[approved denied].each do |decision|
        %w[final_review local_validation field_qa].each do |gate|
          event = human_event({"id" => "HID-EVENT-0099"}, artifact: candidate, decision: decision)
          event["outcome"] = {"gate" => gate, "result" => "pass"}
          error = assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
          assert_equal "OPERATIONAL_FACT_INVALID reserved or unknown event fields", error.message, "#{decision}/#{gate}"
          assert_nil ledger.head
          assert_equal initial, subject_identity(root)
          assert_equal refs, git!(root, "for-each-ref", "--format=%(refname) %(objectname)")
        end
      end
    end
  end

  private

  def projection(validator)
    validator.run.fetch("operational_state").fetch("HID-SHADOW-MODE-001")
  end

  def subject_identity(root)
    identity = Hid::GitFacts.new(root).capture
    identity["index_digest"] = Digest::SHA256.file(File.join(root, ".git/index")).hexdigest
    identity["feature_ref"] = git!(root, "rev-parse", "refs/heads/feature/test").strip
    identity
  end

  def with_real_workspace
    source = File.expand_path("../..", __dir__)
    Dir.mktmpdir("hid-realistic-lifecycle") do |root|
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      # Read-only object sharing makes historical snapshots verifiable without
      # copying or changing any real branch or the real control ledger.
      object_path = File.expand_path(git!(source, "rev-parse", "--git-path", "objects").strip, source)
      File.write(File.join(root, ".git/objects/info/alternates"), "#{object_path}\n")
      FileUtils.cp_r(File.join(source, ".hid"), root)
      manifests = Dir[File.join(root, ".hid/features/*.yaml")].sort.map do |path|
        YAML.safe_load(File.read(path), permitted_classes: [], aliases: false)
      end
      references = state_project.fetch("canonical_authority").values + manifests.flat_map { |manifest| manifest.fetch("canonical").values }
      references.uniq.each do |relative|
        FileUtils.mkdir_p(File.dirname(File.join(root, relative)))
        FileUtils.cp(File.join(source, relative), File.join(root, relative))
      end
      git!(root, "add", ".hid", "docs", "AGENTS.md")
      git!(root, "commit", "-q", "-m", "fixture static requirements")
      git!(root, "branch", "-M", "develop")
      git!(root, "update-ref", "refs/remotes/origin/develop", "HEAD")
      git!(root, "checkout", "-q", "-b", "feature/test")
      File.write(File.join(root, "subject.txt"), "fixture subject\n")
      git!(root, "add", "subject.txt")
      git!(root, "commit", "-q", "-m", "fixture candidate")
      git = Hid::GitFacts.new(root)
      yield root, Hid::Validator.new(root, git: git), git
    end
  end

  def git!(root, *args)
    out, err, status = Open3.capture3("git", "-C", root, *args)
    raise "fixture git failed: #{err}" unless status.success?

    out
  end
end
