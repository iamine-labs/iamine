# frozen_string_literal: true

module Hid
  class StateInvariants
    def initialize(feature, events, project)
      @feature = feature
      @events = events
      @project = project
      @candidate = feature.dig("git", "candidate_snapshot")
    end

    def failures
      requirements = @project.fetch("state_requirements").fetch(@feature.dig("state", "current"), {})
      expected_gates = requirements.fetch("gates", {}).dup
      add_required_gates(expected_gates) if requirements["required_gates"] == "passed"

      gate_failures(expected_gates) + event_failures(Array(requirements["events"]))
    end

    def authorized_gate?(gate_name, rule)
      return false unless rule.is_a?(Hash)

      @events.any? { |event| authorization_matches?(event, gate_name, rule) }
    end

    private

    def add_required_gates(expected_gates)
      @feature.fetch("gates").each do |gate_name, gate|
        expected_gates[gate_name] = "passed" if gate["required"]
      end
    end

    def gate_failures(expected_gates)
      expected_gates.each_with_object([]) do |(gate_name, status), result|
        gate = @feature.dig("gates", gate_name)
        if gate.nil? || gate["status"] != status
          result << "gate #{gate_name} must be #{status}"
          next
        end

        rule = @project.dig("human_gates", gate_name)
        result << "gate #{gate_name} lacks correlated human authorization" if rule && !authorized_gate?(gate_name, rule)
      end
    end

    def event_failures(required_events)
      required_events.each_with_object([]) do |event_name, result|
        exists = @events.any? { |event| event["feature"] == @feature["id"] && event["event"] == event_name }
        result << "event #{event_name} is required" unless exists
      end
    end

    def authorization_matches?(event, gate_name, rule)
      authorization = event["authorization"]
      return false unless event["event"] == "human_authorization"
      return false unless event.dig("actor", "type") == "human"
      return false unless event["feature"] == @feature["id"]
      return false unless authorization.is_a?(Hash)
      return false unless authorization["gate"] == gate_name
      return false unless authorization["action"] == rule["action"]
      return false unless authorization["decision"] == "approved"
      return false unless event.dig("artifact", "dirty") == false
      return true unless rule["artifact_bound"]

      event.dig("artifact", "head_sha") == @candidate["head_sha"] &&
        event.dig("artifact", "tree") == @candidate["tree"]
    end
  end
end
