# frozen_string_literal: true

require_relative "test_helper"

module OperationalFixture
  def operational_feature
    YAML.safe_load(File.read(File.expand_path("../features/HID-SHADOW-MODE-001.yaml", __dir__)), permitted_classes: [], aliases: false)
  end

  def operational_projection(events = [], feature: operational_feature, current: current_candidate, project: state_project, git: nil)
    git ||= FakeGit.new({HidTestCase::HEAD => [:valid, HidTestCase::TREE], HidTestCase::OTHER_HEAD => [:valid, HidTestCase::OTHER_TREE]}, current: current)
    Hid::GateProjection.new(feature, events, project, current: current, git: git).run
  end

  def operational_event(gate, result: "pass", artifact: current_candidate, project: state_project)
    rule = project.fetch("operational_policy").fetch("gate_authorities").fetch(gate)
    name = case gate
           when "implementation" then "implementation_completed"
           when "local_validation" then result == "pass" ? "validation_passed" : "validation_failed"
           when "field_qa" then result == "pass" ? "field_qa_passed" : "field_qa_blocked"
           when "merged" then "merged"
           when "post_merge_validation" then result == "pass" ? "post_merge_validation_passed" : "validation_failed"
           when "closure" then "feature_closed"
           else result == "pass" ? "architecture_approved" : "architecture_changes_required"
           end
    role = project.dig("operational_policy", "mandates", rule["mandate"], "roles").first
    event = operational_envelope(name, artifact: artifact, actor: {"type" => "agent", "role" => role})
    event["outcome"] = {
      "gate" => gate, "phase" => rule["phase"], "result" => result, "mandate" => rule["mandate"],
      "evidence" => {"feature" => event["feature"], "artifact" => event["artifact"].dup,
                     "kind" => rule["phase"], "result" => result, "reference" => "fixture-#{event['id']}"}
    }
    event
  end

  def operational_envelope(name, artifact: current_candidate, actor: {"type" => "agent", "role" => "reviewer"})
    @operational_sequence = (@operational_sequence || 100) + 1
    {
      "schema_version" => "0.0.3", "id" => format("HID-EVENT-%04d", @operational_sequence),
      "ts" => "2026-09-02T04:00:00Z", "project" => "iamine", "feature" => "HID-SHADOW-MODE-001",
      "event" => name, "actor" => actor,
      "artifact" => {"base_sha" => nil, "head_sha" => artifact["head_sha"], "tree" => artifact["tree"], "dirty" => false},
      "control_record" => {"plane" => "control_ledger", "ledger_commit" => "f" * 40}
    }
  end

  def nonhuman_events(artifact: current_candidate, field_qa: false)
    gates = %w[architecture implementation local_validation architecture_checkpoint]
    gates << "field_qa" if field_qa
    (gates + ["final_review"]).map { |gate| operational_event(gate, artifact: artifact) }
  end

  def capsule_event(events, artifact: current_candidate, feature: operational_feature, git: nil)
    eligibility = operational_projection(events, current: artifact, feature: feature, git: git)["human_gate_eligibility"]
    operational_envelope("human_decision_requested", artifact: artifact).merge(
      "capsule" => {"action" => "merge", "target_branch" => "develop", "prerequisite_events" => eligibility["prerequisite_events"] || []}
    )
  end

  def human_event(capsule = nil, artifact: current_candidate, decision: "approved")
    operational_envelope("human_authorization", artifact: artifact, actor: {"type" => "human", "role" => "human"}).merge(
      "authorization" => {"gate" => "human_merge", "action" => "merge", "decision" => decision},
      "capsule_id" => capsule && capsule["id"]
    )
  end

  def approved_events
    events = nonhuman_events
    capsule = capsule_event(events)
    events + [capsule, human_event(capsule)]
  end
end
