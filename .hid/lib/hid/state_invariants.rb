# frozen_string_literal: true

module Hid
  class StateInvariants
    PRIVILEGED_START_STATE = "APPROVED FOR MERGE"
    LIFECYCLE_EVENT_TYPES = %w[merged post_merge_validation_passed feature_closed].freeze
    CONSTITUTIONAL_GATES = %w[local_validation final_review human_merge].freeze
    CONSTITUTIONAL_EVENTS = {
      "APPROVED FOR MERGE" => [],
      "MERGED" => %w[merged],
      "POST-MERGE VALIDATION" => %w[merged post_merge_validation_passed],
      "MERGED / VALIDATED / CLOSED" => %w[merged post_merge_validation_passed feature_closed]
    }.freeze
    CONSTITUTIONAL_HUMAN_GATES = {
      "final_review" => {"action" => "architecture_merge_approval", "artifact_bound" => true},
      "human_merge" => {"action" => "merge", "artifact_bound" => true}
    }.freeze
    EVENT_STATUS_PRIORITY = %i[
      target_mismatch
      candidate_mismatch
      strategy_unsupported
      merge_tree_mismatch
      merge_not_clean
      merge_tree_not_verifiable
      canonical_missing
      canonical_ref_unavailable
      canonical_unknown
      unknown
      invalid
    ].freeze

    def self.privileged_states(lifecycle_states)
      start = lifecycle_states.index(PRIVILEGED_START_STATE)
      start ? lifecycle_states.drop(start) : []
    end

    def self.policy_coverage_failures(privileged_states, requirements)
      privileged_states.reject { |state| requirements.key?(state) }
                       .map { |state| "POLICY_INCOMPLETE state #{state} has no requirements" }
    end

    def self.constitution_coverage_failures(privileged_states)
      privileged_states.reject { |state| CONSTITUTIONAL_EVENTS.key?(state) }
                       .map { |state| "CONSTITUTIONAL_POLICY_INCOMPLETE state #{state} has no minimum requirements" }
    end

    def self.constitutional_policy_failures(privileged_states, requirements, human_gates)
      failures = constitution_coverage_failures(privileged_states)
      privileged_states.each do |state|
        gates = requirements.dig(state, "gates")
        next unless gates.is_a?(Hash)

        CONSTITUTIONAL_GATES.each do |gate_name|
          next unless gates.key?(gate_name) && gates[gate_name] != "passed"

          failures << "CONSTITUTIONAL_POLICY_VIOLATION state #{state} cannot weaken gate #{gate_name}"
        end
      end

      CONSTITUTIONAL_HUMAN_GATES.each do |gate_name, minimum|
        configured = human_gates[gate_name]
        next if configured.is_a?(Hash) && configured["action"] == minimum["action"] &&
                configured["artifact_bound"] == minimum["artifact_bound"]

        failures << "CONSTITUTIONAL_POLICY_VIOLATION human gate #{gate_name} must remain artifact-bound to #{minimum['action']}"
      end
      failures
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
      constitutional = self.class.constitutional_policy_failures(
        privileged_states,
        requirements,
        @project.fetch("human_gates")
      )
      return constitutional unless constitutional.empty?

      expected_gates = constitutional_gates(state).to_h { |gate_name| [gate_name, "passed"] }
      expected_gates.merge!(policy.fetch("gates", {}))
      add_required_gates(expected_gates) if policy["required_gates"] == "passed"
      required_events = (constitutional_events(state) + Array(policy["events"])).uniq
      gate_failures(state, expected_gates) +
        event_failures(required_events) +
        lifecycle_order_failures(required_events, expected_gates)
    end

    def authorization_status(gate_name, rule, before_index: nil)
      return :missing unless rule.is_a?(Hash)
      return :candidate_dirty if @current.nil? || @current["dirty"] != false

      relevant = human_decisions(gate_name, rule, before_index: before_index)
      return :stale if relevant.empty? && any_human_decision?(gate_name, rule, before_index: before_index)
      return :missing if relevant.empty?

      relevant.last.first.dig("authorization", "decision") == "approved" ? :approved : :denied
    end

    def authorization_status_for_state(gate_name, rule)
      return authorization_status(gate_name, rule) unless gate_name == "human_merge" && constitutional_events(current_state).include?("merged")

      valid_merged_events.each do |_event, index|
        status = authorization_status(gate_name, rule, before_index: index)
        return :approved if status == :approved
      end
      authorization_status(gate_name, rule, before_index: first_merged_event_index)
    end

    def authorized_gate?(gate_name, rule)
      authorization_status(gate_name, rule) == :approved
    end

    def valid_supporting_event?(event_name)
      supporting_event_status(event_name) == :valid
    end

    private

    def current_state
      @feature.dig("state", "current")
    end

    def privileged_states
      Array(@project["derived_privileged_states"])
    end

    def add_required_gates(expected_gates)
      @feature.fetch("gates").each do |gate_name, gate|
        expected_gates[gate_name] = "passed" if gate["required"]
      end
    end

    def constitutional_gates(state)
      CONSTITUTIONAL_EVENTS.key?(state) ? CONSTITUTIONAL_GATES : []
    end

    def constitutional_events(state)
      CONSTITUTIONAL_EVENTS.fetch(state, [])
    end

    def gate_failures(state, expected_gates)
      expected_gates.each_with_object([]) do |(gate_name, status), result|
        gate = @feature.dig("gates", gate_name)
        if gate.nil? || gate["status"] != status
          result << "gate #{gate_name} must be #{status}"
          next
        end

        rule = @project.dig("human_gates", gate_name)
        if rule.nil?
          if constitutional_gates(state).include?(gate_name) && CONSTITUTIONAL_HUMAN_GATES.key?(gate_name)
            result << "CONSTITUTIONAL_POLICY_VIOLATION gate #{gate_name} lacks its human authorization rule"
          end
          next
        end

        authorization = authorization_status_for_state(gate_name, rule)
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

    def human_decisions(gate_name, rule, before_index: nil)
      @events.each_with_index.each_with_object([]) do |(event, index), result|
        next if before_index && index >= before_index
        next unless human_decision_for_gate?(event, gate_name, rule) && artifact_matches_current?(event)

        result << [event, index]
      end
    end

    def any_human_decision?(gate_name, rule, before_index: nil)
      @events.each_with_index.any? do |event, index|
        (!before_index || index < before_index) && human_decision_for_gate?(event, gate_name, rule)
      end
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

    def lifecycle_order_failures(required_events, expected_gates)
      sequence = LIFECYCLE_EVENT_TYPES.select { |event_name| required_events.include?(event_name) }
      return [] if sequence.empty?
      return [] unless sequence.all? { |event_name| supporting_event_status(event_name) == :valid }
      required_human_gates = expected_gates.keys.select { |gate_name| @project.dig("human_gates", gate_name) }
      return [] if valid_lifecycle_sequence?(sequence, required_human_gates)

      ["LIFECYCLE_ORDER_VIOLATION expected required human authorization < #{sequence.join(' < ')}"]
    end

    def valid_lifecycle_sequence?(sequence, required_human_gates)
      valid_merged_events.any? do |merged_event, merged_index|
        authorizations_valid = required_human_gates.all? do |gate_name|
          rule = @project.dig("human_gates", gate_name)
          authorization_status(gate_name, rule, before_index: merged_index) == :approved
        end
        next false unless authorizations_valid

        artifact = merged_event["artifact"]
        previous_index = merged_index
        sequence.drop(1).all? do |event_name|
          match = indexed_events(event_name).find do |event, index|
            index > previous_index && same_git_artifact?(event["artifact"], artifact) &&
              supporting_event_artifact_status(event, event_name) == :valid
          end
          previous_index = match.last if match
          !match.nil?
        end
      end
    end

    def valid_merged_events
      indexed_events("merged").select do |event, _index|
        supporting_event_artifact_status(event, "merged") == :valid
      end
    end

    def first_merged_event_index
      indexed_events("merged").first&.last
    end

    def indexed_events(event_name)
      @events.each_with_index.select do |event, _index|
        event["feature"] == @feature["id"] && event["event"] == event_name
      end
    end

    def supporting_event_status(event_name)
      events = @events.select { |event| event["feature"] == @feature["id"] && event["event"] == event_name }
      return :missing if events.empty?

      statuses = events.map { |event| supporting_event_artifact_status(event, event_name) }
      return :valid if statuses.include?(:valid)
      EVENT_STATUS_PRIORITY.find { |status| statuses.include?(status) } || :invalid
    end

    def supporting_event_artifact_status(event, event_name)
      artifact = event["artifact"]
      status = @git.artifact_status(artifact["head_sha"], artifact["tree"])
      return status unless status == :valid

      if event_name == "merged"
        return :invalid if artifact["head_sha"] == @current["head_sha"]

        ancestry = @git.ancestry_status(@current["head_sha"], artifact["head_sha"])
        return :unknown if ancestry == :unknown
        return :invalid unless ancestry == :ancestor

        return integration_event_status(event)
      end

      return status unless LIFECYCLE_EVENT_TYPES.include?(event_name)

      valid_merged_artifact?(artifact) ? :valid : :invalid
    end

    def integration_event_status(event)
      integration = event["integration"]
      return :candidate_mismatch unless integration.is_a?(Hash)
      return :candidate_mismatch unless integration["source_head_sha"] == @current["head_sha"] &&
                                        integration["source_tree"] == @current["tree"]

      canonical_target = @project.dig("project", "integration_branch")
      return :target_mismatch unless integration["target_branch"] == canonical_target

      relation = @git.merge_relation_status(
        @current["head_sha"],
        event.dig("artifact", "head_sha"),
        integration["strategy"]
      )
      return :strategy_unsupported if relation == :unsupported
      return :merge_tree_mismatch if relation == :merge_tree_mismatch
      return :merge_not_clean if relation == :merge_not_clean
      return :merge_tree_not_verifiable if relation == :merge_tree_not_verifiable
      return :canonical_unknown if relation == :unknown
      return :candidate_mismatch unless relation == :valid

      containment = @git.canonical_integration_status(event.dig("artifact", "head_sha"), canonical_target)
      return :valid if containment == :contained
      return :canonical_missing if containment == :not_contained
      return :canonical_ref_unavailable if containment == :unavailable

      :canonical_unknown
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
      when :target_mismatch
        "CANONICAL_TARGET_MISMATCH event #{event_name} does not target the configured integration branch"
      when :candidate_mismatch
        "CANDIDATE_INTEGRATION_MISMATCH event #{event_name} is not a controlled merge of the authorized candidate"
      when :strategy_unsupported
        "INTEGRATION_STRATEGY_UNSUPPORTED event #{event_name} does not use the canonical no-ff merge strategy"
      when :merge_tree_mismatch
        "MERGE_TREE_MISMATCH event #{event_name} tree differs from the deterministic clean merge result"
      when :merge_not_clean
        "MERGE_NOT_CLEAN event #{event_name} candidate and canonical parent do not merge cleanly"
      when :merge_tree_not_verifiable
        "MERGE_TREE_NOT_VERIFIABLE event #{event_name} deterministic merge tree could not be verified"
      when :canonical_missing
        "CANONICAL_INTEGRATION_MISSING event #{event_name} artifact is not contained in the canonical integration branch"
      when :canonical_ref_unavailable
        "CANONICAL_REF_UNAVAILABLE event #{event_name} canonical integration ref is unavailable"
      when :canonical_unknown
        "CANONICAL_INTEGRATION_NOT_VERIFIABLE event #{event_name} canonical integration could not be verified"
      else
        "INVALID_EVENT_ARTIFACT event #{event_name} lacks a valid Git artifact"
      end
    end
  end
end
