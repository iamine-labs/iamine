# frozen_string_literal: true

require "json"
require "time"
require "yaml"

require_relative "git_facts"
require_relative "privacy"
require_relative "state_invariants"

module Hid
  class ValidationError < StandardError; end

  class Validator
    SHA_PATTERN = /\A[0-9a-f]{40}\z/
    FEATURE_ID_PATTERN = /\A[A-Z0-9][A-Z0-9-]*\z/
    EVENT_ID_PATTERN = /\AHID-EVENT-\d{4,}\z/
    GATE_STATUSES = %w[pending not_required passed failed blocked].freeze
    ACTOR_TYPES = %w[human agent system].freeze
    ACTOR_ROLES = %w[architect developer qa reviewer merge-owner human system].freeze
    RESULT_STATUSES = %w[pass fail blocked unknown].freeze
    DATA_CLASSES = %w[SOURCE DERIVED SNAPSHOT].freeze
    GATE_NAMES = %w[architecture local_validation field_qa final_review human_merge].freeze
    GATE_NAME_PATTERN = /\A[a-z][a-z0-9_]*\z/
    BRANCH_NAME_PATTERN = /\A[A-Za-z0-9][A-Za-z0-9._\/-]*\z/

    attr_reader :warnings

    def initialize(root, git: nil)
      @root = root
      @hid_root = File.join(root, ".hid")
      @git = git || GitFacts.new(root)
      @warnings = []
    end

    def run
      project = validate_project(File.join(@hid_root, "project.yaml"))
      privacy = load_privacy(project)
      current = @git.capture

      feature_paths = Dir[File.join(@hid_root, "features", "*.yaml")].sort
      assert(!feature_paths.empty?, "no HID feature manifests found")
      features = feature_paths.map { |path| validate_feature(path, project, privacy) }
      feature_ids = features.map { |feature| feature["id"] }

      template_path = File.join(@hid_root, "templates", "evidence.json")
      validate_evidence_shape(read_json(template_path), template_path, template: true)

      evidence = load_evidence(feature_ids, project, privacy, current)
      events = validate_events(File.join(@hid_root, "events.jsonl"), project, feature_ids, privacy)

      features.each do |feature|
        validate_evidence_references(feature, evidence)
        validate_human_gates(feature, events, project, current)
        validate_state_and_gates(feature, evidence, events, project, current)
      end

      append_only = validate_append_only(File.join(@hid_root, "events.jsonl"))
      next_actions = features.to_h { |feature| [feature["id"], derive_next_action(feature, events, project, current)] }

      {
        "features" => feature_paths.length,
        "evidence" => evidence.length,
        "events" => events.length,
        "current" => current,
        "evidence_statuses" => evidence.transform_values { |entry| entry.fetch("derived_status") },
        "append_only" => append_only,
        "next_actions" => next_actions,
        "warnings" => warnings.dup
      }
    end

    def validate_human_gates(feature, events, project, current = nil)
      current ||= @git.capture
      invariants = StateInvariants.new(feature, events, project, current: current, git: @git)
      project.fetch("human_gates").each do |gate_name, rule|
        gate = feature.fetch("gates").fetch(gate_name)
        next unless gate["status"] == "passed"

        status = invariants.authorization_status_for_state(gate_name, rule)
        assert(status == :approved, "#{feature['id']}: human gate #{gate_name} is not authorized: #{status}")
      end
    end

    def classify_evidence(evidence, current)
      state, actual_tree = @git.commit_tree(evidence.dig("artifact", "head_sha"))
      return "UNKNOWN" if state == :unknown
      return "INVALID" if state == :invalid
      return "INVALID" unless actual_tree == evidence.dig("artifact", "tree")
      return "STALE" if current["dirty"]

      same_artifact = evidence.dig("artifact", "head_sha") == current["head_sha"] &&
                      evidence.dig("artifact", "tree") == current["tree"]
      same_artifact ? "VALID" : "STALE"
    end

    def validate_evidence_references(feature, evidence)
      feature.fetch("evidence").each do |id|
        assert(evidence.key?(id), "#{feature['id']}: referenced evidence #{id} is missing")
      end
    end

    def validate_human_authorization_event(event, project, location)
      assert(event.dig("actor", "type") == "human", "#{location}: human authorization actor.type must be human")
      authorization = fetch(event, "authorization", location)
      assert(authorization.is_a?(Hash), "#{location}: authorization must be an object")
      assert(%w[approved denied].include?(authorization["decision"]), "#{location}: invalid authorization decision")
      gate = authorization["gate"]
      assert(project.fetch("human_gates").key?(gate), "#{location}: unknown human gate")
      assert(project.dig("human_gates", gate, "action") == authorization["action"], "#{location}: action does not match gate")
      assert_sha(event.dig("artifact", "head_sha"), "#{location}: authorization head_sha")
      assert_sha(event.dig("artifact", "tree"), "#{location}: authorization tree")
      assert(event.dig("artifact", "dirty") == false, "#{location}: authorization must bind to a clean artifact")
    end

    def validate_merged_event(event, location)
      integration = fetch(event, "integration", location)
      assert(integration.is_a?(Hash), "#{location}: integration must be an object")
      assert_sha(integration["source_head_sha"], "#{location}: integration.source_head_sha")
      assert_sha(integration["source_tree"], "#{location}: integration.source_tree")
      target = integration["target_branch"]
      assert(target.is_a?(String) && BRANCH_NAME_PATTERN.match?(target), "#{location}: invalid integration.target_branch")
      assert(integration["strategy"].is_a?(String), "#{location}: integration.strategy is required")
    end

    def validate_append_only_prefix(baseline, candidate, location)
      assert(candidate.start_with?(baseline), "#{location}: committed baseline events were modified or removed")
      "baseline_prefix_preserved"
    end

    private

    def assert(condition, message)
      raise ValidationError, message unless condition
    end

    def fetch(hash, key, path)
      assert(hash.key?(key), "#{path}: missing #{key}")
      hash[key]
    end

    def read_yaml(path)
      value = YAML.safe_load(File.read(path), permitted_classes: [], aliases: false)
      assert(value.is_a?(Hash), "#{path}: expected a YAML object")
      value
    rescue Psych::Exception => e
      raise ValidationError, "#{path}: invalid YAML: #{e.message}"
    end

    def read_json(path)
      value = JSON.parse(File.read(path))
      assert(value.is_a?(Hash), "#{path}: expected a JSON object")
      value
    rescue JSON::ParserError => e
      raise ValidationError, "#{path}: invalid JSON: #{e.message}"
    end

    def assert_sha(value, path, allow_nil: false)
      return if allow_nil && value.nil?

      assert(value.is_a?(String) && SHA_PATTERN.match?(value), "#{path}: expected a 40-character lowercase Git SHA")
    end

    def workflow_states(path)
      body = File.read(path)
      match = body.match(/## Canonical States\s+```text\s+(.*?)```/m)
      assert(match, "#{path}: canonical state block not found")
      match[1].lines.map(&:strip).reject(&:empty?)
    end

    def validate_project(path)
      project = read_yaml(path)
      assert(project["schema_version"] == "0.0.2", "#{path}: unsupported schema_version")
      assert(project.dig("project", "id") == "iamine", "#{path}: project.id must be iamine")
      assert(project.dig("project", "integration_branch") == "develop", "#{path}: integration branch must be develop")
      assert(project.dig("mode", "name") == "shadow", "#{path}: mode must be shadow")
      assert(project.dig("mode", "enforcement") == false, "#{path}: shadow enforcement must be false")
      assert(project.dig("mode", "canonical_workflow_wins") == true, "#{path}: canonical workflow must win")
      assert(project.dig("authority", "silence_is_authorization") == false, "#{path}: silence cannot authorize")

      canonical = fetch(project, "canonical_authority", path)
      canonical.each_value do |relative_path|
        assert(relative_path.is_a?(String), "#{path}: canonical references must be strings")
        assert(File.file?(File.join(@root, relative_path)), "#{path}: missing canonical reference #{relative_path}")
      end

      expected_states = workflow_states(File.join(@root, canonical.fetch("workflow")))
      assert(project.dig("lifecycle", "representation") == "derived_at_validation", "#{path}: lifecycle must be derived")
      project["derived_lifecycle_states"] = expected_states
      privileged_states = StateInvariants.privileged_states(expected_states)
      assert(!privileged_states.empty?, "#{path}: POLICY_INCOMPLETE privileged lifecycle boundary is missing")
      project["derived_privileged_states"] = privileged_states

      DATA_CLASSES.each do |data_class|
        assert(project.dig("data_semantics", data_class).is_a?(Array), "#{path}: missing data semantics for #{data_class}")
      end
      assert(project.dig("events", "append_only") == "policy", "#{path}: append-only must be described as policy")
      assert(project.dig("events", "baseline_unavailable") == "visible_not_checked", "#{path}: unavailable baseline must be visible")
      assert(project.dig("risk_gates", "enforcement") == false, "#{path}: risk gates cannot enforce in v0.0.2")
      assert(project.dig("model_routing", "enforced") == false, "#{path}: model routing cannot be enforced in v0.0.2")

      human_gates = fetch(project, "human_gates", path)
      human_gates.each do |name, rule|
        assert(rule["action"].is_a?(String), "#{path}: human gate #{name} requires an action")
        assert([true, false].include?(rule["artifact_bound"]), "#{path}: human gate #{name} artifact_bound must be boolean")
      end

      state_requirements = fetch(project, "state_requirements", path)
      coverage_failures = StateInvariants.policy_coverage_failures(privileged_states, state_requirements)
      assert(coverage_failures.empty?, "#{path}: #{coverage_failures.join('; ')}")
      constitutional_failures = StateInvariants.constitutional_policy_failures(
        privileged_states,
        state_requirements,
        human_gates
      )
      assert(constitutional_failures.empty?, "#{path}: #{constitutional_failures.join('; ')}")
      state_requirements.each do |state, requirements|
        assert(expected_states.include?(state), "#{path}: state requirement references non-canonical state #{state}")
        assert(requirements.is_a?(Hash), "#{path}: state requirement #{state} must be an object")
        assert([nil, "passed"].include?(requirements["required_gates"]), "#{path}: invalid required_gates rule for #{state}")
        fetch(requirements, "gates", "#{path}: state_requirements.#{state}").each do |gate_name, status|
          assert(GATE_NAME_PATTERN.match?(gate_name), "#{path}: state requirement #{state} has invalid gate name #{gate_name}")
          assert(status == "passed", "#{path}: state requirement #{state}.#{gate_name} must require passed")
        end
        Array(requirements["events"]).each do |event_name|
          assert(project.dig("events", "allowed").include?(event_name), "#{path}: state requirement #{state} references unknown event #{event_name}")
        end
      end
      project
    end

    def load_privacy(project)
      relative = project.dig("privacy", "policy")
      assert(relative.is_a?(String), ".hid/project.yaml: privacy.policy is required")
      path = File.join(@root, relative)
      assert(File.file?(path), ".hid/project.yaml: missing privacy policy #{relative}")
      policy = read_yaml(path)
      assert(policy["schema_version"] == "0.0.2", "#{path}: unsupported schema_version")
      %w[ALLOW REDACT NEVER_STORE].each { |tier| assert(policy[tier].is_a?(Hash), "#{path}: missing #{tier}") }
      PrivacyPolicy.new(policy)
    end

    def validate_feature(path, project, privacy)
      feature = read_yaml(path)
      assert(feature["schema_version"] == "0.0.2", "#{path}: unsupported schema_version")
      id = fetch(feature, "id", path)
      assert(FEATURE_ID_PATTERN.match?(id), "#{path}: invalid feature id")
      assert(File.basename(path, ".yaml") == id, "#{path}: filename must match feature id")
      assert(project.fetch("derived_lifecycle_states").include?(feature.dig("state", "current")), "#{path}: non-canonical current state")
      assert(feature.dig("state", "data_class") == "SOURCE", "#{path}: state must be classified as SOURCE")
      assert(project.fetch("risk_levels").key?(feature.dig("risk", "level")), "#{path}: invalid risk level")

      fetch(feature, "canonical", path).each_value do |relative_path|
        assert(File.file?(File.join(@root, relative_path)), "#{path}: missing feature reference #{relative_path}")
      end

      base = feature.dig("git", "base_snapshot")
      candidate = feature.dig("git", "candidate_snapshot")
      validate_snapshot(base, "#{path}: git.base_snapshot")
      validate_snapshot(candidate, "#{path}: git.candidate_snapshot")
      assert(base["branch"] == "develop", "#{path}: base branch must be develop")
      assert(feature.dig("git", "current", "data_class") == "DERIVED", "#{path}: git.current must be DERIVED")

      state, actual_tree = @git.commit_tree(candidate["head_sha"])
      assert(state != :invalid, "#{path}: candidate commit does not exist")
      assert(state != :unknown, "#{path}: candidate commit could not be verified")
      assert(actual_tree == candidate["tree"], "#{path}: candidate commit/tree mismatch")

      gates = fetch(feature, "gates", path)
      GATE_NAMES.each { |gate_name| fetch(gates, gate_name, path) }
      gates.each do |gate_name, gate|
        assert(GATE_NAME_PATTERN.match?(gate_name), "#{path}: invalid gate name #{gate_name}")
        assert([true, false].include?(gate["required"]), "#{path}: #{gate_name}.required must be boolean")
        assert(GATE_STATUSES.include?(gate["status"]), "#{path}: invalid #{gate_name}.status")
      end

      assert(feature["evidence"].is_a?(Array), "#{path}: evidence must be an array")
      assert(feature["evidence"].all? { |id_value| id_value.is_a?(String) }, "#{path}: evidence IDs must be strings")
      assert(feature["blockers"].is_a?(Array), "#{path}: blockers must be an array")
      assert(feature.dig("next_action", "data_class") == "DERIVED", "#{path}: next_action must be DERIVED")
      assert(feature.dig("next_action", "manual_override").nil?, "#{path}: next_action manual override is not authorized")
      validate_usage(feature.dig("execution", "usage"), path)
      enforce_privacy(privacy.findings(feature, path))
      feature
    rescue ArgumentError => e
      raise ValidationError, "#{path}: invalid timestamp: #{e.message}"
    end

    def validate_snapshot(snapshot, path)
      assert(snapshot.is_a?(Hash), "#{path}: expected an object")
      assert(snapshot["data_class"] == "SNAPSHOT", "#{path}: data_class must be SNAPSHOT")
      assert_sha(snapshot["head_sha"], "#{path}.head_sha")
      assert_sha(snapshot["tree"], "#{path}.tree")
      assert([true, false].include?(snapshot["dirty"]), "#{path}.dirty must be boolean")
      Time.iso8601(snapshot["captured_at"])
    end

    def validate_usage(usage, path)
      assert(usage.is_a?(Hash), "#{path}: execution.usage must be an object")
      usage.each do |name, value|
        valid = value == "not_measured" || (value.is_a?(Integer) && value >= 0)
        assert(valid, "#{path}: execution.usage.#{name} must be not_measured or a nonnegative integer")
      end
    end

    def load_evidence(feature_ids, project, privacy, current)
      result = {}
      Dir[File.join(@hid_root, "evidence", "*.json")].sort.each do |path|
        evidence = read_json(path)
        validate_evidence_shape(evidence, path)
        assert(feature_ids.include?(evidence["feature"]), "#{path}: unknown feature")
        assert(File.basename(path, ".json") == evidence["id"], "#{path}: filename must match evidence id")
        assert(!result.key?(evidence["id"]), "#{path}: duplicate evidence id")
        assert(evidence["failure_class"].nil? || project["failure_classes"].include?(evidence["failure_class"]), "#{path}: invalid failure_class")
        enforce_privacy(privacy.findings(evidence, path))

        status = classify_evidence(evidence, current)
        evidence["derived_status"] = status
        assert(status != "INVALID", "#{path}: evidence is INVALID")
        warnings << "evidence_status #{evidence['id']}=#{status}" if %w[STALE UNKNOWN].include?(status)
        result[evidence["id"]] = evidence
      end
      result
    end

    def validate_evidence_shape(evidence, path, template: false)
      assert(%w[0.0.1 0.0.2].include?(evidence["schema_version"]), "#{path}: unsupported schema_version")
      return if template && evidence.dig("artifact", "head_sha").nil?

      assert_sha(evidence.dig("artifact", "head_sha"), "#{path}: artifact.head_sha")
      assert_sha(evidence.dig("artifact", "tree"), "#{path}: artifact.tree")
      assert(RESULT_STATUSES.include?(evidence.dig("result", "status")), "#{path}: invalid result status")
      assert(evidence.dig("environment", "host_class").is_a?(String), "#{path}: environment.host_class is required")
      assert(evidence.dig("execution", "commands").is_a?(Array), "#{path}: execution.commands must be an array")
      started = Time.iso8601(evidence.dig("execution", "started_at"))
      finished = Time.iso8601(evidence.dig("execution", "finished_at"))
      assert(started <= finished, "#{path}: execution timestamps are reversed")

      return unless evidence["schema_version"] == "0.0.2"

      Time.iso8601(evidence["captured_at"])
      assert(evidence.dig("coverage", "paths").is_a?(Array), "#{path}: coverage.paths must be an array")
      assert(evidence.dig("coverage", "claims").is_a?(Array), "#{path}: coverage.claims must be an array")
      assert(evidence["dependencies"].is_a?(Array), "#{path}: dependencies must be an array")
      assert([true, false].include?(evidence.dig("validity", "artifact_bound")), "#{path}: validity.artifact_bound must be boolean")
      assert([true, false].include?(evidence.dig("validity", "environment_bound")), "#{path}: validity.environment_bound must be boolean")
    rescue ArgumentError => e
      raise ValidationError, "#{path}: invalid timestamp: #{e.message}"
    end

    def validate_artifact(artifact, path)
      assert(artifact.is_a?(Hash), "#{path}: artifact must be an object")
      %w[base_sha head_sha tree].each do |key|
        assert_sha(artifact[key], "#{path}: artifact.#{key}", allow_nil: true)
      end
      assert([true, false].include?(artifact["dirty"]), "#{path}: artifact.dirty must be boolean")
    end

    def validate_events(path, project, feature_ids, privacy)
      allowed = project.dig("events", "allowed")
      ids = {}
      previous_time = nil
      events = []

      File.foreach(path).with_index(1) do |line, line_number|
        next if line.strip.empty?

        event = JSON.parse(line)
        location = "#{path}:#{line_number}"
        assert(event.is_a?(Hash), "#{location}: event must be an object")
        assert(%w[0.0.1 0.0.2].include?(event["schema_version"]), "#{location}: unsupported schema_version")
        assert(EVENT_ID_PATTERN.match?(event["id"].to_s), "#{location}: invalid event id")
        assert(event["project"] == "iamine", "#{location}: project must be iamine")
        assert(feature_ids.include?(event["feature"]), "#{location}: unknown feature")
        assert(allowed.include?(event["event"]), "#{location}: unknown event type")
        assert(!ids.key?(event["id"]), "#{location}: duplicate event id")
        ids[event["id"]] = true

        timestamp = Time.iso8601(event["ts"])
        assert(previous_time.nil? || timestamp >= previous_time, "#{location}: timestamps must be nondecreasing")
        previous_time = timestamp
        assert(ACTOR_TYPES.include?(event.dig("actor", "type")), "#{location}: invalid actor type")
        assert(ACTOR_ROLES.include?(event.dig("actor", "role")), "#{location}: invalid actor role")
        validate_artifact(fetch(event, "artifact", location), location)
        validate_human_authorization_event(event, project, location) if event["event"] == "human_authorization"
        validate_merged_event(event, location) if event["event"] == "merged"
        enforce_privacy(privacy.findings(event, location))
        events << event
      rescue JSON::ParserError, ArgumentError => e
        raise ValidationError, "#{location}: invalid event: #{e.message}"
      end
      events
    end

    def validate_state_and_gates(feature, evidence, events, project, current = nil)
      current ||= @git.capture
      candidate = feature.dig("git", "candidate_snapshot")
      matching_evidence = feature["evidence"].any? do |id|
        record = evidence[id]
        record && record["derived_status"] != "INVALID" &&
          record.dig("artifact", "head_sha") == candidate["head_sha"] &&
          record.dig("artifact", "tree") == candidate["tree"]
      end

      if feature.dig("gates", "local_validation", "status") == "passed"
        assert(matching_evidence, "#{feature['id']}: local validation passed without evidence for candidate snapshot")
      end

      failures = state_requirement_failures(feature, events, project, current)
      assert(failures.empty?, "#{feature['id']}: STATE_GATE_INCONSISTENCY: #{failures.join('; ')}")
    end

    def derive_next_action(feature, events, project, current = nil)
      current ||= @git.capture
      failures = state_requirement_failures(feature, events, project, current)
      return "policy_incomplete" if failures.any? { |failure| failure.start_with?("POLICY_INCOMPLETE") }
      return "constitutional_policy_violation" if failures.any? { |failure| failure.start_with?("CONSTITUTIONAL_POLICY") }
      return "canonical_integration_not_verifiable" if failures.any? do |failure|
        failure.start_with?("CANONICAL_REF_UNAVAILABLE", "CANONICAL_INTEGRATION_NOT_VERIFIABLE")
      end
      return "canonical_integration_missing" if failures.any? do |failure|
        failure.start_with?(
          "CANONICAL_INTEGRATION_MISSING",
          "CANONICAL_TARGET_MISMATCH",
          "CANDIDATE_INTEGRATION_MISMATCH",
          "INTEGRATION_STRATEGY_UNSUPPORTED"
        )
      end
      return "lifecycle_inconsistency" if failures.any? { |failure| failure.start_with?("LIFECYCLE_ORDER_VIOLATION") }
      return "state_gate_inconsistency" unless failures.empty?
      return "resolve_blockers" unless feature["blockers"].empty?

      case feature.dig("state", "current")
      when "CHANGES REQUIRED"
        "implement_required_changes"
      when "ARCHITECTURE REVIEW REQUIRED"
        "request_architecture_review"
      when "READY FOR MERGE REVIEW"
        "request_human_merge_decision"
      when "APPROVED FOR MERGE"
        "run_merge_precheck"
      else
        "follow_canonical_workflow"
      end
    end

    def validate_append_only(path)
      state, previous = @git.baseline_file(".hid/events.jsonl")
      if state == :unavailable
        warnings << "append_only=not_checked reason=git_base_unavailable"
        return "not_checked"
      end
      if state == :missing
        warnings << "append_only=not_checked reason=initial_log"
        return "not_checked"
      end

      current = File.read(path)
      validate_append_only_prefix(previous, current, path)
    end

    def state_requirement_failures(feature, events, project, current)
      StateInvariants.new(feature, events, project, current: current, git: @git).failures
    end

    def enforce_privacy(findings)
      findings.each do |finding|
        message = "#{finding.level} #{finding.kind} at #{finding.source}:#{finding.path}"
        if finding.level == "privacy_violation"
          raise ValidationError, message
        else
          warnings << message
        end
      end
    end
  end
end
