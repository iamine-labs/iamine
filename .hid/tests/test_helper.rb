# frozen_string_literal: true

require "minitest/autorun"
require "yaml"
require_relative "../lib/hid/validator"

class FakeGit
  def initialize(trees = {}, baseline: [:unavailable, nil])
    @trees = trees
    @baseline = baseline
  end

  def commit_tree(sha)
    @trees.fetch(sha, [:invalid, nil])
  end

  def baseline_file(_path)
    @baseline
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
    @validator = Hid::Validator.new(Dir.pwd, git: FakeGit.new(HEAD => [:valid, TREE]))
  end

  private

  def state_project
    @state_project ||= YAML.safe_load(File.read(File.expand_path("../project.yaml", __dir__)), permitted_classes: [], aliases: false)
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

  def validate_state(feature, events)
    evidence = {
      "HID-EVID-0001" => {
        "derived_status" => "VALID",
        "artifact" => {"head_sha" => HEAD, "tree" => TREE}
      }
    }
    @validator.send(:validate_state_and_gates, feature, evidence, events, state_project)
  end

  def derive_next_action(feature, events)
    @validator.send(:derive_next_action, feature, events, state_project)
  end

  def authorization_events
    [
      authorization_event(gate: "architecture", action: "development_authorization"),
      authorization_event(gate: "final_review", action: "architecture_merge_approval"),
      authorization_event
    ]
  end

  def authorization_event(gate: "human_merge", action: "merge")
    {
      "ts" => "2026-08-30T00:00:00Z",
      "event" => "human_authorization",
      "feature" => "HID-SHADOW-MODE-001",
      "actor" => {"type" => "human", "role" => "merge-owner"},
      "authorization" => {"gate" => gate, "action" => action, "decision" => "approved"},
      "artifact" => {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}
    }
  end

  def workflow_event(name)
    {
      "event" => name,
      "feature" => "HID-SHADOW-MODE-001",
      "artifact" => {"head_sha" => HEAD, "tree" => TREE, "dirty" => false}
    }
  end

  def evidence_record(head, tree)
    {"artifact" => {"head_sha" => head, "tree" => tree}}
  end

  def privacy_findings(value)
    policy = Hid::PrivacyPolicy.load(File.expand_path("../privacy.yaml", __dir__))
    policy.findings(value, "fixture")
  end
end
