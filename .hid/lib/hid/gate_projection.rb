# frozen_string_literal: true

require_relative "operational_facts"
require_relative "lifecycle_projection"

module Hid
  class GateProjection
    CANONICAL_ORDER = %w[architecture implementation local_validation architecture_checkpoint field_qa final_review human_merge].freeze
    PENDING_ACTIONS = {
      "architecture" => ["ARCHITECTURE IN PROGRESS", "request_architecture_review"],
      "implementation" => ["DEVELOPMENT AUTHORIZED", "implement_feature"],
      "local_validation" => ["IMPLEMENTATION COMPLETE", "run_local_validation"],
      "architecture_checkpoint" => ["ARCHITECTURE REVIEW REQUIRED", "request_architecture_checkpoint"],
      "field_qa" => ["FIELD QA AUTHORIZED", "run_field_qa"],
      "final_review" => ["READY FOR MERGE REVIEW", "request_final_review"],
      "human_merge" => ["READY FOR MERGE REVIEW", "request_human_gate"]
    }.freeze

    def initialize(feature, events, project, current:, git:)
      @feature, @events, @project, @current, @git = feature, events, project, current, git
      @facts = OperationalFacts.new(project)
    end

    def run(lifecycle: true)
      failures = policy_failures + capsule_failures
      gates = nonhuman_gates(@events)
      gates["human_merge"] = human_outcome(gates)
      eligibility = eligibility_for(gates)
      state, action = state_action(gates)
      if state == "APPROVED FOR MERGE"
        projected = @feature.merge("state" => {"current" => state}, "gates" => gates)
        failures.concat(StateInvariants.new(projected, @events, @project, current: @current, git: @git).failures)
      end
      unless failures.empty?
        state, action = "CHANGES REQUIRED", "policy_incomplete"
        eligibility = {"status" => "NOT_READY", "reason" => "APPROVAL_CAPSULE_NOT_READY", "missing_gate" => "policy"}
      end
      result = {"gates" => gates, "state" => state, "next_action" => action,
                "human_gate_eligibility" => eligibility, "failures" => failures}
      result = LifecycleProjection.new(self, @feature, @events, @project, @current, @git).apply(result) if lifecycle
      result["warnings"] = legacy_mismatches(result)
      result
    end

    def human_gate_eligibility
      run(lifecycle: false)["human_gate_eligibility"]
    end

    def prefix(events)
      self.class.new(@feature, events, @project, current: @current, git: @git).run(lifecycle: false)
    end

    def current_event?(event)
      @facts.operational?(event) && event["feature"] == @feature["id"] &&
        @facts.same_artifact?(event["artifact"], @current)
    end

    def capsule_failures
      @events.each_with_index.each_with_object([]) do |(event, index), failures|
        next unless current_event?(event)

        if event["event"] == "human_authorization" && event.dig("authorization", "decision") == "approved"
          prefix = @events.take(index + 1)
          projection = self.class.new(@feature, prefix, @project, current: @current, git: @git)
          outcome = projection.send(:human_outcome, projection.send(:nonhuman_gates, prefix))
          failures << "APPROVAL_CAPSULE_NOT_READY #{event['id']}" unless outcome["status"] == "passed"
        end
        next unless event["event"] == "human_decision_requested"

        eligibility = eligibility_for(nonhuman_gates(@events.take(index)))
        unless eligibility["status"] == "READY" && event.dig("capsule", "prerequisite_events") == eligibility["prerequisite_events"]
          failures << "APPROVAL_CAPSULE_NOT_READY #{event['id']}"
        end
      end
    end

    private

    def policy_failures
      failures = @facts.policy_failures + StateInvariants.policy_coverage_failures(
        @project.fetch("derived_privileged_states"), @project.fetch("state_requirements")
      ) + StateInvariants.constitutional_policy_failures(
        @project.fetch("derived_privileged_states"), @project.fetch("state_requirements"), @project.fetch("human_gates")
      )
      unless Array(@project.dig("state_requirements", "APPROVED FOR MERGE", "events")).empty?
        failures << "POLICY_INCOMPLETE pre-merge event requirements need explicit typed gates"
      end
      failures
    end

    def gate_order
      extra = (@feature.fetch("gates").keys + policy_gates - CANONICAL_ORDER).uniq.sort
      CANONICAL_ORDER.take(5) + extra + %w[final_review human_merge]
    end

    def policy_gates
      @project.fetch("derived_privileged_states").flat_map do |state|
        @project.dig("state_requirements", state, "gates").to_h.keys
      end.uniq
    end

    def requirement(gate)
      value = @feature.dig("gates", gate, "required")
      return nil unless [true, false].include?(value)

      value || StateInvariants::CONSTITUTIONAL_GATES.include?(gate) || policy_gates.include?(gate)
    end

    def nonhuman_gates(events)
      previous_index = -1
      gate_order.reject { |gate| gate == "human_merge" }.each_with_object({}) do |gate, outcomes|
        required = requirement(gate)
        status = required.nil? ? "unknown" : required ? "pending" : "not_required"
        result = {"required" => required, "status" => status}
        if required
          candidates = events.each_with_index.select { |event, _index| current_event?(event) && event.dig("outcome", "gate") == gate }
          unless candidates.empty?
            event, index = candidates.last
            @facts.validate!(event)
            result.merge!("status" => {"pass" => "passed", "fail" => "failed", "blocked" => "blocked"}.fetch(event.dig("outcome", "result")),
                          "event_id" => event["id"], "event_index" => index)
            if index <= previous_index
              result.merge!("status" => "blocked", "reason" => "GATE_ORDER_VIOLATION")
            end
            previous_index = [index, previous_index].max
          end
          result["status"] = "unknown" unless @project.dig("operational_policy", "gate_authorities", gate).is_a?(Hash)
        end
        outcomes[gate] = result
      end
    end

    def eligibility_for(gates)
      missing = gates.find { |gate, outcome| gate != "human_merge" && !%w[passed not_required].include?(outcome["status"]) }
      missing_gate = missing && missing[0]
      missing_gate ||= "candidate" unless @current["dirty"] == false &&
        @git.artifact_status(@current["head_sha"], @current["tree"]) == :valid
      missing_gate ||= "policy" unless policy_failures.empty?
      if missing_gate
        {"status" => "NOT_READY", "reason" => "APPROVAL_CAPSULE_NOT_READY", "missing_gate" => missing_gate}
      else
        {"status" => "READY", "prerequisite_events" => gates.reject { |gate, _| gate == "human_merge" }.values.map { |outcome| outcome["event_id"] }.compact}
      end
    end

    def human_outcome(gates)
      result = {"required" => true, "status" => "pending"}
      if requirement("human_merge").nil?
        return result.merge("status" => "unknown")
      end
      decisions = @events.each_with_index.select { |event, _| current_event?(event) && event["event"] == "human_authorization" }
      return result if decisions.empty?

      event, index = decisions.last
      @facts.validate!(event)
      result["event_id"] = event["id"]
      return result.merge("status" => "failed", "reason" => "AUTHORIZATION_DENIED") if event.dig("authorization", "decision") == "denied"

      capsule_index = @events.take(index).rindex { |prior| current_event?(prior) && prior["event"] == "human_decision_requested" && prior["id"] == event["capsule_id"] }
      return result.merge("status" => "blocked", "reason" => "APPROVAL_CAPSULE_NOT_READY") unless capsule_index

      capsule = @events[capsule_index]
      snapshots = [nonhuman_gates(@events.take(capsule_index)), nonhuman_gates(@events.take(index)), gates]
      valid = snapshots.all? do |snapshot|
        eligibility = eligibility_for(snapshot)
        eligibility["status"] == "READY" && eligibility["prerequisite_events"] == capsule.dig("capsule", "prerequisite_events")
      end
      result.merge("status" => valid ? "passed" : "blocked", "reason" => valid ? "exact_capsule_approval" : "AUTHORIZATION_REQUIRES_NEW_CAPSULE")
    end

    def state_action(gates)
      missing = gates.find { |_gate, outcome| !%w[passed not_required].include?(outcome["status"]) }
      return ["CHANGES REQUIRED", "capture_clean_candidate"] if @current["dirty"] != false
      return ["APPROVED FOR MERGE", "run_merge_precheck"] unless missing

      gate, outcome = missing
      return ["CHANGES REQUIRED", "policy_incomplete"] if outcome["status"] == "unknown"
      if %w[failed blocked].include?(outcome["status"])
        return [gate == "field_qa" ? "QA BLOCKED" : "CHANGES REQUIRED", "resolve_#{gate}"]
      end
      PENDING_ACTIONS.fetch(gate, ["ARCHITECTURE REVIEW REQUIRED", "request_#{gate}"])
    end

    def legacy_mismatches(result)
      mismatches = result["gates"].map do |gate, outcome|
        "legacy_status_mismatch gate=#{gate}" if @feature.dig("gates", gate, "status") != outcome["status"]
      end.compact
      mismatches << "legacy_state_mismatch" if @feature.dig("state", "current") != result["state"]
      mismatches
    end
  end
end
