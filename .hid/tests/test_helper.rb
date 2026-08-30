# frozen_string_literal: true

require "minitest/autorun"
require "yaml"
require_relative "../lib/hid/validator"

class FakeGit
  def initialize(trees = {}, baseline: [:unavailable, nil], current: nil, ancestries: {})
    @trees = trees
    @baseline = baseline
    @ancestries = ancestries
    head = trees.keys.first
    @current = current || {"branch" => "fixture", "head_sha" => head, "tree" => trees.dig(head, 1), "dirty" => false}
  end

  def capture
    @current
  end

  def commit_tree(sha)
    @trees.fetch(sha, [:invalid, nil])
  end

  def baseline_file(_path)
    @baseline
  end

  def artifact_status(head_sha, expected_tree)
    state, actual_tree = commit_tree(head_sha)
    return state unless state == :valid

    actual_tree == expected_tree ? :valid : :invalid
  end

  def ancestry_status(ancestor_sha, descendant_sha)
    @ancestries.fetch([ancestor_sha, descendant_sha], ancestor_sha == descendant_sha ? :ancestor : :unrelated)
  end
end

class HidTestCase < Minitest::Test
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
    @validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new(HEAD => [:valid, TREE], OTHER_HEAD => [:valid, OTHER_TREE]))
  end

  private

  def state_project
    @state_project ||= begin
      project = YAML.safe_load(File.read(File.expand_path("../project.yaml", __dir__)), permitted_classes: [], aliases: false)
      workflow = File.read(File.expand_path("../../docs/process/iamine-canonical-workflow.md", __dir__))
      states = workflow.match(/## Canonical States\s+```text\s+(.*?)```/m)[1].lines.map(&:strip).reject(&:empty?)
      project["derived_lifecycle_states"] = states
      project["derived_privileged_states"] = Hid::StateInvariants.privileged_states(states)
      project
    end
  end

  def state_feature(state, overrides = {})
    statuses = {
      "architecture" => "passed",
      "local_validation" => "passed",
      "field_qa" => "not_required",
      "final_review" => "passed",
      "human_merge" => "passed"
    }.merge(overrides)
    required = {
      "architecture" => true,
      "local_validation" => true,
      "field_qa" => false,
      "final_review" => true,
      "human_merge" => true
    }

    {
      "id" => "HID-SHADOW-MODE-001",
      "state" => {"current" => state},
      "git" => {"candidate_snapshot" => {"head_sha" => HEAD, "tree" => TREE}},
      "gates" => statuses.to_h { |name, status| [name, {"required" => required.fetch(name), "status" => status}] },
      "evidence" => ["HID-EVID-0001"],
      "blockers" => []
    }
  end

  def validate_state(feature, events, current: current_candidate, validator: @validator, evidence: nil)
    evidence ||= {
      "HID-EVID-0001" => {
        "derived_status" => "VALID",
        "artifact" => {"head_sha" => HEAD, "tree" => TREE}
      }
    }
    validator.send(:validate_state_and_gates, feature, evidence, events, state_project, current)
  end

  def derive_next_action(feature, events, current: current_candidate, project: state_project)
    @validator.send(:derive_next_action, feature, events, project, current)
  end

  def authorization_events(head: HEAD, tree: TREE)
    [
      authorization_event(gate: "architecture", action: "development_authorization", head: head, tree: tree),
      authorization_event(gate: "final_review", action: "architecture_merge_approval", head: head, tree: tree),
      authorization_event(head: head, tree: tree)
    ]
  end

  def authorization_event(gate: "human_merge", action: "merge", decision: "approved", head: HEAD, tree: TREE, actor: "human")
    {
      "ts" => "2026-08-30T00:00:00Z",
      "event" => "human_authorization",
      "feature" => "HID-SHADOW-MODE-001",
      "actor" => {"type" => actor, "role" => "merge-owner"},
      "authorization" => {"gate" => gate, "action" => action, "decision" => decision},
      "artifact" => {"head_sha" => head, "tree" => tree, "dirty" => false}
    }
  end

  def workflow_event(name, head: HEAD, tree: TREE)
    {
      "event" => name,
      "feature" => "HID-SHADOW-MODE-001",
      "artifact" => {"head_sha" => head, "tree" => tree, "dirty" => false}
    }
  end

  def current_candidate(head: HEAD, tree: TREE, dirty: false)
    {"branch" => "feature/test", "head_sha" => head, "tree" => tree, "dirty" => dirty}
  end

  def evidence_record(head, tree)
    {"artifact" => {"head_sha" => head, "tree" => tree}}
  end

  def privacy_findings(value)
    policy = Hid::PrivacyPolicy.load(File.expand_path("../privacy.yaml", __dir__))
    policy.findings(value, "fixture")
  end
end
