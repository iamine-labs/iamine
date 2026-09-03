# frozen_string_literal: true

module Hid
  # Candidate identity stays fixed; integration identity is obtained from Git,
  # never by promoting the source commit or trusting a lifecycle label.
  class LifecycleProjection
    def initialize(projection, feature, events, project, current, git)
      @projection, @feature, @events, @project, @current, @git = projection, feature, events, project, current, git
      @facts = OperationalFacts.new(project)
    end

    def apply(result)
      merged = @events.each_with_index.find do |event, _index|
        relevant?(event) && event["event"] == "merged" &&
          event.dig("integration", "source_head_sha") == @current["head_sha"]
      end
      return result unless merged

      event, index = merged
      before = @projection.prefix(@events.take(index))
      return invalid(result, ["LIFECYCLE_ORDER_VIOLATION merge prerequisites missing"]) unless
        before["state"] == "APPROVED FOR MERGE" && before["failures"].empty?

      errors = invariant_failures("MERGED", before["gates"], @events.take(index + 1))
      return invalid(result, errors) unless errors.empty?

      result = result.merge("gates" => before["gates"], "state" => "MERGED", "next_action" => "run_post_merge_validation")
      result["human_gate_eligibility"] = {"status" => "NOT_READY", "reason" => "ALREADY_MERGED"}
      integration = event["artifact"]
      post_passed = false
      closed = false
      @events.each_with_index do |later, later_index|
        next unless relevant?(later) && %w[post_merge_validation closure].include?(later.dig("outcome", "gate"))
        next unless @facts.same_artifact?(later["artifact"], integration)

        return invalid(result, ["LIFECYCLE_ORDER_VIOLATION post-merge event ordering"]) if later_index <= index || closed
        if later.dig("outcome", "gate") == "post_merge_validation"
          post_passed = later.dig("outcome", "result") == "pass"
          state = post_passed ? "POST-MERGE VALIDATION" : "CHANGES REQUIRED"
          action = post_passed ? "request_architecture_closure" : "resolve_post_merge_validation"
        else
          return invalid(result, ["LIFECYCLE_ORDER_VIOLATION closure requires post-merge validation"]) unless post_passed

          state, action, closed = "MERGED / VALIDATED / CLOSED", "none", true
        end
        errors = invariant_failures(state, before["gates"], @events.take(later_index + 1))
        return invalid(result, errors) unless errors.empty?

        result = result.merge("state" => state, "next_action" => action)
      end
      result
    end

    private

    def relevant?(event)
      @facts.operational?(event) && event["feature"] == @feature["id"]
    end

    def invariant_failures(state, gates, events)
      projected = @feature.merge("state" => {"current" => state}, "gates" => gates)
      StateInvariants.new(projected, events, @project, current: @current, git: @git).failures
    end

    def invalid(result, errors)
      result.merge("state" => "CHANGES REQUIRED", "next_action" => "lifecycle_inconsistency",
                   "human_gate_eligibility" => {"status" => "NOT_READY", "reason" => "LIFECYCLE_INCONSISTENCY"},
                   "failures" => result["failures"] + errors)
    end
  end
end
