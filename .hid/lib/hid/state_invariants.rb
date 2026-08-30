# frozen_string_literal: true

module Hid
  class StateInvariants
    PRIVILEGED_START_STATE = "APPROVED FOR MERGE"
    LIFECYCLE_EVENT_TYPES = %w[merged post_merge_validation_passed feature_closed].freeze

    def self.privileged_states(lifecycle_states)
      start = lifecycle_states.index(PRIVILEGED_START_STATE)
      start ? lifecycle_states.drop(start) : []
    end

    def self.policy_coverage_failures(privileged_states, requirements)
      privileged_states.reject { |state| requirements.key?(state) }
                       .map { |state| "POLICY_INCOMPLETE state #{state} has no requirements" }
    end

    def initialize(feature, events, project, current:, git:)
      @feature = feature
      @events = events
      @project = project
      @current = current
      @git = git
    end

    def failures
      state = @feature.dig("state", "current")
      requirements = @project.fetch("state_requirements")
      coverage = self.class.policy_coverage_failures(privileged_states, requirements)
      return coverage if privileged_states.include?(state) && !requirements.key?(state)

      policy = requirements.fetch(state, {})
      expected_gates = policy.fetch("gates", {}).dup
      add_required_gates(expected_gates) if policy["required_gates"] == "passed"
      gate_failures(expected_gates) + event_failures(Array(policy["events"]))
    end

    def authorization_status(gate_name, rule)
      return :missing unless rule.is_a?(Hash)
      return :candidate_dirty if @current.nil? || @current["dirty"] != false

      relevant = human_decisions(gate_name, rule)
      return :stale if relevant.empty? && any_human_decision?(gate_name, rule)
      return :missing if relevant.empty?

      relevant.last.dig("authorization", "decision") == "approved" ? :approved : :denied
    end

    def authorized_gate?(gate_name, rule)
      authorization_status(gate_name, rule) == :approved
    end

    def valid_supporting_event?(event_name)
      supporting_event_status(event_name) == :valid
    end

    private

    def privileged_states
      Array(@project["derived_privileged_states"])
    end

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
        next unless rule

        authorization = authorization_status(gate_name, rule)
        result << authorization_failure(gate_name, authorization) unless authorization == :approved
      end
    end

    def authorization_failure(gate_name, status)
      case status
      when :stale
        "AUTHORIZATION_STALE gate #{gate_name} does not target current candidate"
      when :denied
        "AUTHORIZATION_DENIED gate #{gate_name} latest relevant decision is denied"
      when :candidate_dirty
        "AUTHORIZATION_STALE gate #{gate_name} current candidate is dirty"
      else
        "gate #{gate_name} lacks correlated human authorization"
      end
    end

    def human_decisions(gate_name, rule)
      @events.select do |event|
        human_decision_for_gate?(event, gate_name, rule) && artifact_matches_current?(event)
      end
    end

    def any_human_decision?(gate_name, rule)
      @events.any? { |event| human_decision_for_gate?(event, gate_name, rule) }
    end

    def human_decision_for_gate?(event, gate_name, rule)
      authorization = event["authorization"]
      event["event"] == "human_authorization" &&
        event.dig("actor", "type") == "human" &&
        event["feature"] == @feature["id"] &&
        authorization.is_a?(Hash) &&
        authorization["gate"] == gate_name &&
        authorization["action"] == rule["action"] &&
        %w[approved denied].include?(authorization["decision"]) &&
        event.dig("artifact", "dirty") == false
    end

    def artifact_matches_current?(event)
      event.dig("artifact", "head_sha") == @current["head_sha"] &&
        event.dig("artifact", "tree") == @current["tree"]
    end

    def event_failures(required_events)
      required_events.each_with_object([]) do |event_name, result|
        status = supporting_event_status(event_name)
        next if status == :valid

        result << event_failure(event_name, status)
      end
    end

    def supporting_event_status(event_name)
      events = @events.select { |event| event["feature"] == @feature["id"] && event["event"] == event_name }
      return :missing if events.empty?

      statuses = events.map { |event| supporting_event_artifact_status(event, event_name) }
      return :valid if statuses.include?(:valid)
      return :unknown if statuses.include?(:unknown)

      :invalid
    end

    def supporting_event_artifact_status(event, event_name)
      artifact = event["artifact"]
      status = @git.artifact_status(artifact["head_sha"], artifact["tree"])
      return status unless status == :valid

      if event_name == "merged"
        return :invalid if artifact["head_sha"] == @current["head_sha"]

        ancestry = @git.ancestry_status(@current["head_sha"], artifact["head_sha"])
        return :valid if ancestry == :ancestor
        return :unknown if ancestry == :unknown

        return :invalid
      end

      return status unless LIFECYCLE_EVENT_TYPES.include?(event_name)

      valid_merged_artifact?(artifact) ? :valid : :invalid
    end

    def valid_merged_artifact?(artifact)
      @events.any? do |event|
        next false unless event["feature"] == @feature["id"] && event["event"] == "merged"
        next false unless same_git_artifact?(event["artifact"], artifact)

        supporting_event_artifact_status(event, "merged") == :valid
      end
    end

    def same_git_artifact?(left, right)
      left["head_sha"] == right["head_sha"] && left["tree"] == right["tree"]
    end

    def event_failure(event_name, status)
      case status
      when :missing
        "event #{event_name} is required"
      when :unknown
        "EVENT_ARTIFACT_UNKNOWN event #{event_name} could not be verified"
      else
        "INVALID_EVENT_ARTIFACT event #{event_name} lacks a valid Git artifact"
      end
    end
  end
end
