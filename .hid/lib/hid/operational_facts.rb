# frozen_string_literal: true

module Hid
  # v0.0.9 adds typed operational facts; older records remain immutable history.
  class OperationalFacts
    SCHEMA = "0.0.3"
    MINIMUM_RULES = {
      "architecture" => ["review", "development_authorization", "architect"],
      "implementation" => ["implementation", "implementation", "developer"],
      "local_validation" => ["validation", "local_validation", "developer"],
      "architecture_checkpoint" => ["review", "architecture_checkpoint", "architect"],
      "field_qa" => ["validation", "field_qa", "qa"],
      "final_review" => ["review", "final_review", "architect"],
      "merged" => ["integration", "merged", "merge-owner"],
      "post_merge_validation" => ["validation", "post_merge_validation", "qa"],
      "closure" => ["review", "closure", "architect"]
    }.freeze
    EVENT_RESULTS = {
      "architecture_approved" => ["review", %w[pass]],
      "architecture_changes_required" => ["review", %w[fail blocked]],
      "implementation_completed" => ["implementation", %w[pass]],
      "validation_passed" => ["validation", %w[pass]],
      "validation_failed" => ["validation", %w[fail blocked]],
      "field_qa_passed" => ["validation", %w[pass]],
      "field_qa_blocked" => ["validation", %w[fail blocked]],
      "merged" => ["integration", %w[pass]],
      "post_merge_validation_passed" => ["validation", %w[pass]],
      "feature_closed" => ["review", %w[pass]]
    }.freeze
    EXCLUSIVE_EVENTS = {
      "field_qa_passed" => "field_qa", "field_qa_blocked" => "field_qa",
      "merged" => "merged", "post_merge_validation_passed" => "post_merge_validation",
      "feature_closed" => "closure"
    }.freeze

    def initialize(project)
      @project = project
    end

    def policy_failures
      policy = @project["operational_policy"]
      return ["POLICY_INCOMPLETE operational policy missing"] unless policy.is_a?(Hash)

      failures = []
      failures << "POLICY_INCOMPLETE operational cutover" unless policy["version"] == "0.0.9" &&
        policy["legacy_runtime_fields"] == "non_authoritative"
      MINIMUM_RULES.each do |gate, (kind, phase, role)|
        rule = policy.dig("gate_authorities", gate)
        mandate = policy.dig("mandates", rule && rule["mandate"])
        valid = rule.is_a?(Hash) && rule["kind"] == kind && rule["phase"] == phase &&
          mandate.is_a?(Hash) && mandate["roles"] == [role] &&
          mandate["actor_types"].is_a?(Array) && !mandate["actor_types"].empty? &&
          (mandate["actor_types"] - %w[agent human]).empty? &&
          mandate["gates"].is_a?(Array) && mandate["gates"].include?(gate) &&
          mandate["features"].is_a?(Array) && !mandate["features"].empty?
        failures << "CONSTITUTIONAL_POLICY_VIOLATION review/operational authority #{gate}" unless valid
      end
      if (@project.fetch("human_gates", {}).keys & MINIMUM_RULES.keys).any?
        failures << "CONSTITUTIONAL_POLICY_VIOLATION review authority cannot be human merge authority"
      end
      failures
    end

    def operational?(event)
      event["schema_version"] == SCHEMA && event.dig("control_record", "plane") == "control_ledger"
    end

    def validate!(event)
      check(event["schema_version"] == SCHEMA, "operational schema required")
      check(JSON.generate(event).bytesize <= 16_384, "operational event exceeds size limit")
      check(event["id"].is_a?(String) && /\AHID-EVENT-\d{4,}\z/.match?(event["id"]), "invalid event id")
      check(event["feature"].is_a?(String), "feature required")
      artifact = event["artifact"]
      check(artifact.is_a?(Hash) && artifact["dirty"] == false &&
        %w[head_sha tree].all? { |key| GitFacts::SHA_PATTERN.match?(artifact[key].to_s) }, "exact clean artifact required")
      return validate_human_event!(event) if %w[human_authorization human_decision_requested].include?(event["event"])

      outcome = event["outcome"]
      check(outcome.is_a?(Hash), "typed outcome required")
      rule = @project.dig("operational_policy", "gate_authorities", outcome["gate"])
      check(rule.is_a?(Hash), "unknown outcome gate")
      check(outcome["phase"] == rule["phase"], "wrong review/validation phase")
      expected = EVENT_RESULTS[event["event"]]
      check(expected && expected[0] == rule["kind"] && expected[1].include?(outcome["result"]), "event/result does not match gate")
      exclusive = EXCLUSIVE_EVENTS[event["event"]]
      check(exclusive.nil? || exclusive == outcome["gate"], "wrong lifecycle gate")
      if EXCLUSIVE_EVENTS.values.include?(outcome["gate"])
        post_failure = outcome["gate"] == "post_merge_validation" && event["event"] == "validation_failed"
        check(exclusive == outcome["gate"] || post_failure, "gate requires its canonical event")
      end
      mandate = @project.dig("operational_policy", "mandates", outcome["mandate"])
      check(outcome["mandate"] == rule["mandate"] && mandate.is_a?(Hash), "reviewer mandate missing or wrong")
      check(Array(mandate["gates"]).include?(outcome["gate"]) &&
        Array(mandate["features"]).include?(event["feature"]) &&
        Array(mandate["actor_types"]).include?(event.dig("actor", "type")) &&
        Array(mandate["roles"]).include?(event.dig("actor", "role")), "actor not authorized by mandate")
      validate_evidence!(event, outcome)
    end

    def same_artifact?(left, right)
      left.is_a?(Hash) && right.is_a?(Hash) && left["head_sha"] == right["head_sha"] &&
        left["tree"] == right["tree"] && left["dirty"] == false && right["dirty"] == false
    end

    private

    def validate_human_event!(event)
      if event["event"] == "human_decision_requested"
        capsule = event["capsule"]
        check(capsule.is_a?(Hash) && capsule["action"] == "merge" &&
          capsule["target_branch"] == @project.dig("project", "integration_branch") &&
          capsule["prerequisite_events"].is_a?(Array), "invalid approval capsule request")
      else
        authorization = event["authorization"]
        check(event.dig("actor", "type") == "human", "human authorization requires human actor")
        check(authorization.is_a?(Hash) && authorization["gate"] == "human_merge" &&
          authorization["action"] == "merge" && %w[approved denied].include?(authorization["decision"]), "invalid human merge decision")
        check(authorization["decision"] == "denied" || event["capsule_id"].is_a?(String), "approval requires capsule reference")
      end
    end

    def validate_evidence!(event, outcome)
      evidence = outcome["evidence"]
      check(evidence.is_a?(Hash), "external evidence required")
      check(evidence["feature"] == event["feature"] && same_artifact?(evidence["artifact"], event["artifact"]), "evidence feature/artifact mismatch")
      check(evidence["kind"] == outcome["phase"] && evidence["result"] == outcome["result"], "evidence type/result mismatch")
      reference = evidence["reference"]
      check(reference.is_a?(String) && /\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z/.match?(reference), "bounded evidence reference required")
    end

    def check(condition, message)
      raise ValidationError, "OPERATIONAL_FACT_INVALID #{message}" unless condition
    end
  end
end
