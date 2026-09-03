# frozen_string_literal: true

module Hid
  # Only OperationalFacts constructs these snapshots, after semantic validation.
  OperationalFact = Struct.new(
    :feature, :subject, :domain, :gate, :result, :phase, :authority, :evidence,
    :source_event, :capsule, :capsule_id, :integration, keyword_init: true
  ) do
    def initialize(**attributes)
      super
      each_pair { |_key, value| freeze_snapshot(value) }
      freeze
    end

    def id
      source_event.fetch("id")
    end

    private

    def freeze_snapshot(value)
      case value
      when Hash then value.each { |key, item| freeze_snapshot(key); freeze_snapshot(item) }
      when Array then value.each { |item| freeze_snapshot(item) }
      end
      value.freeze
    end
  end
end
