# frozen_string_literal: true

module Hid
  module AuthorityDomains
    EVENT_DOMAINS = {
      "human_authorization" => "HUMAN_DECISION",
      "human_decision_requested" => "CAPSULE_REQUEST",
      "architecture_approved" => "REVIEW_VERDICT",
      "architecture_changes_required" => "REVIEW_VERDICT",
      "validation_passed" => "VALIDATION_RESULT",
      "validation_failed" => "VALIDATION_RESULT",
      "post_merge_validation_passed" => "VALIDATION_RESULT",
      "field_qa_passed" => "QA_RESULT",
      "field_qa_blocked" => "QA_RESULT",
      "implementation_completed" => "LIFECYCLE_FACT",
      "feature_closed" => "LIFECYCLE_FACT",
      "merged" => "INTEGRATION_FACT"
    }.freeze
    KIND_DOMAINS = {
      "review" => "REVIEW_VERDICT", "validation" => "VALIDATION_RESULT",
      "implementation" => "LIFECYCLE_FACT", "integration" => "INTEGRATION_FACT"
    }.freeze
    GATE_DOMAINS = {"field_qa" => "QA_RESULT", "closure" => "LIFECYCLE_FACT"}.freeze
    COMMON_FIELDS = %w[schema_version id ts project feature event actor artifact control_record metadata].freeze

    def self.expected_kind(gate, rule)
      GATE_DOMAINS.fetch(gate) { KIND_DOMAINS[rule["kind"]] }
    end

    def self.payload_fields(event_type)
      case event_type
      when "human_authorization" then %w[authorization capsule_id]
      when "human_decision_requested" then %w[capsule]
      when "merged" then %w[outcome integration]
      else %w[outcome]
      end
    end
  end
end
