# frozen_string_literal: true

require "tmpdir"
require "fileutils"
require_relative "test_helper"

class HidControlLedgerTest < HidTestCase
  CONTROL_REF = "refs/heads/hid/control-plane"

  def test_human_authorization_is_external_to_the_authorized_candidate
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      before = subject_snapshot(root)
      refs_before = subject_refs(root)

      ledger_before = ledger.head
      ledger_after = ledger.append(JSON.generate(authorization_record(60, candidate)))
      events = validated_control_events(root, ledger.entries)
      status = authorization_status(root, candidate, events)

      assert_nil ledger_before
      assert_match(/\A[0-9a-f]{40}\z/, ledger_after)
      assert_equal before, subject_snapshot(root)
      assert_equal refs_before, subject_refs(root)
      assert_equal :approved, status
      assert_equal candidate.fetch(:head), events.first.dig("artifact", "head_sha")
      assert_equal ledger_after, events.first.dig("control_record", "ledger_commit")
    end
  end

  def test_stale_expected_head_fails_closed
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      ledger.append(JSON.generate(authorization_record(60, candidate)))

      error = assert_raises(Hid::ControlLedgerChanged) do
        ledger.append(JSON.generate(authorization_record(61, candidate, decision: "denied")), expected_head: nil)
      end

      assert_equal "CONTROL_LEDGER_CHANGED", error.message
      assert_equal 1, ledger.entries.length
    end
  end

  def test_supported_writer_preserves_subject_identity
    with_hid_workspace do |root, validator, candidate|
      ledger = control_ledger(root)
      before = subject_snapshot(root)

      event = authorization_record(60, candidate, decision: "denied").merge("schema_version" => "0.0.3")
      ledger_commit = validator.append_control_event(event)

      assert_equal before, subject_snapshot(root)
      assert_equal ledger_commit, ledger.head
      assert_equal 1, ledger.entries.length
    end
  end

  def test_supported_writer_rejects_private_content_before_persistence
    with_hid_workspace do |root, validator, candidate|
      ledger = control_ledger(root)
      event = authorization_record(60, candidate, decision: "denied").merge("schema_version" => "0.0.3")
      event["metadata"] = {"prompt" => "should-never-be-stored"}

      assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
      assert_nil ledger.head
      assert_equal "", git!(root, "status", "--porcelain=v1")
    end
  end

  def test_latest_ledger_decision_controls_revocation_regardless_of_timestamp
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      ledger.append(JSON.generate(authorization_record(60, candidate, ts: "2026-09-01T03:00:00Z")))
      ledger.append(JSON.generate(authorization_record(61, candidate, decision: "denied", ts: "2026-09-01T02:00:00Z")))
      denied_events = validated_control_events(root, ledger.entries)

      assert_equal :denied, authorization_status(root, candidate, denied_events)

      ledger.append(JSON.generate(authorization_record(62, candidate, ts: "2026-09-01T01:00:00Z")))
      approved_events = validated_control_events(root, ledger.entries)

      assert_equal :approved, authorization_status(root, candidate, approved_events)
    end
  end

  def test_synthetic_invariants_preserve_canonical_and_control_refs
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      ledger.append(JSON.generate(authorization_record(62, candidate)))
      candidate_after_authorization = subject_snapshot(root)

      git!(root, "checkout", "-q", "develop")
      git!(root, "merge", "--no-ff", "-q", "feature/test", "-m", "canonical merge")
      integration = git_artifact(root)
      git!(root, "checkout", "-q", "feature/test")
      refs_after_merge = subject_refs(root)

      ledger.append(JSON.generate(lifecycle_record(63, "merged", integration, candidate)))
      ledger.append(JSON.generate(lifecycle_record(64, "post_merge_validation_passed", integration, candidate)))
      ledger.append(JSON.generate(lifecycle_record(65, "feature_closed", integration, candidate)))
      events = validated_control_events(root, ledger.entries)

      validator = Hid::Validator.new(root)
      feature = state_feature("MERGED / VALIDATED / CLOSED")
      feature["git"]["candidate_snapshot"] = {"head_sha" => candidate.fetch(:head), "tree" => candidate.fetch(:tree)}
      current = current_candidate(head: candidate.fetch(:head), tree: candidate.fetch(:tree))
      evidence = {
        "HID-EVID-0001" => {
          "derived_status" => "VALID",
          "artifact" => {"head_sha" => candidate.fetch(:head), "tree" => candidate.fetch(:tree)}
        }
      }

      validate_state(feature, events, current: current, validator: validator, evidence: evidence)
      assert_equal candidate_after_authorization, subject_snapshot(root)
      assert_equal refs_after_merge, subject_refs(root)
      assert_equal integration, git_artifact_for(root, integration.fetch(:head))
      assert_equal 4, ledger.entries.length
    end
  end

  def test_control_ref_does_not_satisfy_canonical_containment
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      ledger.append(JSON.generate(authorization_record(60, candidate)))

      assert_equal :not_contained, ledger.containment_status("develop")
      assert_equal :not_contained, Hid::GitFacts.new(root).canonical_integration_status(ledger.head, "develop")
    end
  end

  def test_historical_control_commit_contained_in_develop_is_detected
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      contaminated_commit = ledger.append(JSON.generate(authorization_record(60, candidate)))
      git!(root, "checkout", "-q", "develop")
      git!(root, "merge", "--allow-unrelated-histories", "--no-ff", "-q", contaminated_commit, "-m", "invalid control merge")
      git!(root, "checkout", "-q", "feature/test")
      ledger.append(JSON.generate(authorization_record(61, candidate, decision: "denied")))

      assert_equal :contained, ledger.containment_status("develop")
    end
  end

  def test_rewritten_control_prefix_is_rejected
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      first = ledger.append(JSON.generate(authorization_record(60, candidate)))
      rewritten = JSON.generate(authorization_record(61, candidate, decision: "denied")) + "\n"
      replacement = raw_control_commit(root, rewritten, parent: first)
      git!(root, "update-ref", CONTROL_REF, replacement, first)

      error = assert_raises(Hid::ControlLedgerError) { ledger.entries }
      assert_includes error.message, "append exactly one event"
    end
  end

  def test_privacy_policy_applies_to_live_control_events
    with_repository do |root, candidate|
      ledger = control_ledger(root)
      before = subject_snapshot(root)
      event = authorization_record(60, candidate)
      event["metadata"] = {"prompt" => "should-never-be-stored"}
      proposed = Hid::ControlLedger::Entry.new(json: JSON.generate(event), ledger_commit: "0" * 40)

      assert_raises(Hid::ValidationError) { validated_control_events(root, [proposed]) }
      assert_nil ledger.head
      assert_equal before, subject_snapshot(root)
    end
  end

  private

  def with_repository
    Dir.mktmpdir("hid-control-ledger") do |root|
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      commit_fixture(root, "base.txt", "base", "base")
      git!(root, "branch", "-M", "develop")
      git!(root, "checkout", "-q", "-b", "feature/test")
      commit_fixture(root, "candidate.txt", "candidate", "candidate")
      yield root, git_artifact(root)
    end
  end

  def with_hid_workspace
    source_root = File.expand_path("../..", __dir__)
    Dir.mktmpdir("hid-control-workspace") do |root|
      FileUtils.cp_r(File.join(source_root, ".hid"), root)
      FileUtils.cp_r(File.join(source_root, "docs"), root)
      FileUtils.cp(File.join(source_root, "AGENTS.md"), root)
      git!(root, "init", "-q")
      git!(root, "config", "user.name", "HID Fixture")
      git!(root, "config", "user.email", "fixture@example.invalid")
      git!(root, "add", ".hid", "docs", "AGENTS.md")
      git!(root, "commit", "-q", "-m", "subject")
      git!(root, "branch", "-M", "develop")
      git!(root, "checkout", "-q", "-b", "feature/test")

      candidate = git_artifact(root)
      manifest = YAML.safe_load(File.read(File.join(root, ".hid/features/HID-SHADOW-MODE-001.yaml")), permitted_classes: [], aliases: false)
      snapshot = manifest.dig("git", "candidate_snapshot")
      current = {
        "branch" => "feature/test",
        "head_sha" => candidate.fetch(:head),
        "tree" => candidate.fetch(:tree),
        "dirty" => false
      }
      git = FakeGit.new(
        {
          snapshot.fetch("head_sha") => [:valid, snapshot.fetch("tree")],
          candidate.fetch(:head) => [:valid, candidate.fetch(:tree)]
        },
        current: current
      )
      yield root, Hid::Validator.new(root, git: git), candidate
    end
  end

  def control_ledger(root)
    Hid::ControlLedger.new(root, ref: CONTROL_REF, events_path: "events.jsonl")
  end

  def authorization_record(id, candidate, gate: "human_merge", action: "merge", decision: "approved", ts: "2026-09-01T00:00:00Z")
    event_record(id, "human_authorization", candidate, ts: ts).merge(
      "authorization" => {"gate" => gate, "action" => action, "decision" => decision}
    )
  end

  def lifecycle_record(id, name, integration, candidate)
    event = event_record(id, name, integration, actor_type: "agent", actor_role: "merge-owner")
    if name == "merged"
      event["integration"] = {
        "source_head_sha" => candidate.fetch(:head),
        "source_tree" => candidate.fetch(:tree),
        "target_branch" => "develop",
        "strategy" => "no_ff_merge"
      }
    end
    event
  end

  def event_record(id, name, artifact, ts: "2026-09-01T00:00:00Z", actor_type: "human", actor_role: "human")
    {
      "schema_version" => "0.0.2",
      "id" => format("HID-EVENT-%04d", id),
      "ts" => ts,
      "project" => "iamine",
      "feature" => "HID-SHADOW-MODE-001",
      "event" => name,
      "actor" => {"type" => actor_type, "role" => actor_role},
      "artifact" => {
        "base_sha" => nil,
        "head_sha" => artifact.fetch(:head),
        "tree" => artifact.fetch(:tree),
        "dirty" => false
      }
    }
  end

  def validated_control_events(root, entries)
    Dir.mktmpdir("hid-empty-baseline") do |dir|
      path = File.join(dir, "events.jsonl")
      File.write(path, "")
      privacy = Hid::PrivacyPolicy.load(File.expand_path("../privacy.yaml", __dir__))
      validator = Hid::Validator.new(root)
      return validator.send(
        :validate_events,
        path,
        state_project,
        ["HID-SHADOW-MODE-001"],
        privacy,
        live_entries: entries
      )
    end
  end

  def authorization_status(root, candidate, events)
    current = current_candidate(head: candidate.fetch(:head), tree: candidate.fetch(:tree))
    invariants = Hid::StateInvariants.new(
      state_feature("APPROVED FOR MERGE"),
      events,
      state_project,
      current: current,
      git: Hid::GitFacts.new(root)
    )
    invariants.authorization_status("human_merge", state_project.dig("human_gates", "human_merge"))
  end

  def subject_snapshot(root)
    {
      head: git!(root, "rev-parse", "HEAD").strip,
      tree: git!(root, "rev-parse", "HEAD^{tree}").strip,
      branch: git!(root, "branch", "--show-current").strip,
      status: git!(root, "status", "--porcelain=v1")
    }
  end

  def subject_refs(root)
    {
      feature: git!(root, "rev-parse", "refs/heads/feature/test").strip,
      develop: git!(root, "rev-parse", "refs/heads/develop").strip
    }
  end

  def git_artifact(root)
    {head: git!(root, "rev-parse", "HEAD").strip, tree: git!(root, "rev-parse", "HEAD^{tree}").strip}
  end

  def git_artifact_for(root, head)
    {head: head, tree: git!(root, "rev-parse", "#{head}^{tree}").strip}
  end

  def raw_control_commit(root, content, parent: nil)
    blob = git_input!(root, content, "hash-object", "-w", "--stdin").strip
    tree = git_input!(root, "100644 blob #{blob}\tevents.jsonl\n", "mktree").strip
    args = ["commit-tree", tree]
    args.concat(["-p", parent]) if parent
    args.concat(["-F", "-"])
    git_input!(root, "fixture control event\n", *args).strip
  end

  def commit_fixture(root, path, content, message)
    File.write(File.join(root, path), content)
    git!(root, "add", path)
    git!(root, "commit", "-q", "-m", message)
  end

  def git!(root, *args)
    stdout, stderr, status = Open3.capture3("git", "-C", root, *args)
    raise "git #{args.join(' ')} failed: #{stderr}" unless status.success?

    stdout
  end

  def git_input!(root, input, *args)
    stdout, stderr, status = Open3.capture3("git", "-C", root, *args, stdin_data: input)
    raise "git #{args.join(' ')} failed: #{stderr}" unless status.success?

    stdout
  end
end
