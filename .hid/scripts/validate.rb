#!/usr/bin/env ruby

require "json"
require "open3"
require "time"
require "yaml"

ROOT = File.expand_path("../..", __dir__)
HID_ROOT = File.join(ROOT, ".hid")
SHA_PATTERN = /\A[0-9a-f]{40}\z/
FEATURE_ID_PATTERN = /\A[A-Z0-9][A-Z0-9-]*\z/
GATE_STATUSES = %w[pending not_required passed failed blocked].freeze
ACTOR_TYPES = %w[human agent system].freeze
ACTOR_ROLES = %w[architect developer qa reviewer merge-owner human system].freeze
RESULT_STATUSES = %w[pass fail blocked unknown].freeze

class HidValidationError < StandardError; end

def fail_validation(message)
  raise HidValidationError, message
end

def assert(condition, message)
  fail_validation(message) unless condition
end

def read_yaml(path)
  value = YAML.safe_load(File.read(path), permitted_classes: [], aliases: false)
  assert(value.is_a?(Hash), "#{path}: expected a YAML object")
  value
rescue Psych::Exception => e
  fail_validation("#{path}: invalid YAML: #{e.message}")
end

def read_json(path)
  value = JSON.parse(File.read(path))
  assert(value.is_a?(Hash), "#{path}: expected a JSON object")
  value
rescue JSON::ParserError => e
  fail_validation("#{path}: invalid JSON: #{e.message}")
end

def fetch(hash, key, path)
  assert(hash.key?(key), "#{path}: missing #{key}")
  hash[key]
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

def each_leaf(value, path = [], &block)
  case value
  when Hash
    value.each { |key, child| each_leaf(child, path + [key.to_s], &block) }
  when Array
    value.each_with_index { |child, index| each_leaf(child, path + [index.to_s], &block) }
  else
    block.call(path, value)
  end
end

def check_sensitive_values(value, source)
  each_leaf(value) do |path, leaf|
    key = path.last.to_s
    if key.match?(/\A(password|token|secret|private_key|credential|credentials)\z/i)
      fail_validation("#{source}: prohibited sensitive key #{path.join('.')}")
    end
    next unless leaf.is_a?(String)

    checks = {
      "personal filesystem path" => %r{/(Users|home)/[^/\s]+/},
      "private key material" => /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
      "token-like value" => /\bsk-[A-Za-z0-9_-]{12,}/,
      "IPv4 address" => /\b(?:\d{1,3}\.){3}\d{1,3}\b/,
      "MAC address" => /\b(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}\b/
    }
    checks.each do |label, pattern|
      fail_validation("#{source}: prohibited #{label} at #{path.join('.')}") if pattern.match?(leaf)
    end
  end
end

def validate_project(path)
  project = read_yaml(path)
  assert(fetch(project, "schema_version", path) == "0.0.1", "#{path}: unsupported schema_version")
  assert(project.dig("project", "id") == "iamine", "#{path}: project.id must be iamine")
  assert(project.dig("project", "integration_branch") == "develop", "#{path}: integration branch must be develop")
  assert(project.dig("mode", "name") == "shadow", "#{path}: mode must be shadow")
  assert(project.dig("mode", "enforcement") == false, "#{path}: shadow enforcement must be false")
  assert(project.dig("mode", "canonical_workflow_wins") == true, "#{path}: canonical workflow must win")
  assert(project.dig("authority", "silence_is_authorization") == false, "#{path}: silence cannot authorize")

  canonical = fetch(project, "canonical_authority", path)
  canonical.each_value do |relative_path|
    assert(relative_path.is_a?(String), "#{path}: canonical references must be strings")
    assert(File.file?(File.join(ROOT, relative_path)), "#{path}: missing canonical reference #{relative_path}")
  end

  expected_states = workflow_states(File.join(ROOT, canonical.fetch("workflow")))
  configured_states = project.dig("lifecycle", "canonical_states")
  assert(configured_states == expected_states, "#{path}: lifecycle states drift from canonical workflow")

  assert(project.dig("git", "evidence_binds_to_tree") == true, "#{path}: evidence must bind to tree")
  assert(project.dig("git", "tree_change_marks_evidence_stale") == true, "#{path}: tree changes must mark evidence stale")
  assert(project.dig("events", "append_only") == true, "#{path}: events must be append-only")
  assert(project.dig("risk_gates", "enforcement") == false, "#{path}: risk gates cannot enforce in v0.0.1")
  assert(project.dig("model_routing", "enforced") == false, "#{path}: model routing cannot be enforced in v0.0.1")
  project
end

def validate_feature(path, project)
  feature = read_yaml(path)
  id = fetch(feature, "id", path)
  assert(FEATURE_ID_PATTERN.match?(id), "#{path}: invalid feature id")
  assert(File.basename(path, ".yaml") == id, "#{path}: filename must match feature id")
  assert(feature.dig("state", "current").is_a?(String), "#{path}: state.current is required")
  assert(project.dig("lifecycle", "canonical_states").include?(feature.dig("state", "current")), "#{path}: non-canonical current state")
  assert(project["risk_levels"].include?(feature.dig("risk", "level")), "#{path}: invalid risk level")
  assert(feature.dig("git", "base_branch") == "develop", "#{path}: base branch must be develop")
  assert_sha(feature.dig("git", "base_sha"), "#{path}: git.base_sha")
  assert_sha(feature.dig("git", "base_tree"), "#{path}: git.base_tree")
  assert_sha(feature.dig("git", "last_capture", "head_sha"), "#{path}: git.last_capture.head_sha")
  assert_sha(feature.dig("git", "last_capture", "tree"), "#{path}: git.last_capture.tree")
  assert([true, false].include?(feature.dig("git", "last_capture", "dirty")), "#{path}: git.last_capture.dirty must be boolean")
  Time.iso8601(feature.dig("git", "last_capture", "captured_at"))

  gates = fetch(feature, "gates", path)
  %w[architecture local_validation field_qa final_review human_merge].each do |gate_name|
    gate = fetch(gates, gate_name, path)
    assert([true, false].include?(gate["required"]), "#{path}: #{gate_name}.required must be boolean")
    assert(GATE_STATUSES.include?(gate["status"]), "#{path}: invalid #{gate_name}.status")
  end

  assert(feature["evidence"].is_a?(Array), "#{path}: evidence must be an array")
  assert(feature["blockers"].is_a?(Array), "#{path}: blockers must be an array")
  assert(feature.dig("next_action", "type").is_a?(String), "#{path}: next_action.type is required")
  assert(feature.dig("execution", "usage", "total_tokens") == "not_measured", "#{path}: unavailable token usage must be not_measured")
  check_sensitive_values(feature, path)
  feature
rescue ArgumentError => e
  fail_validation("#{path}: invalid timestamp: #{e.message}")
end

def validate_artifact(artifact, path)
  %w[base_sha head_sha tree].each do |key|
    assert_sha(artifact[key], "#{path}: artifact.#{key}", allow_nil: true)
  end
  assert([true, false].include?(artifact["dirty"]), "#{path}: artifact.dirty must be boolean")
end

def validate_events(path, project, feature_ids)
  allowed = project.dig("events", "allowed")
  ids = {}
  previous_time = nil
  count = 0

  File.foreach(path).with_index(1) do |line, line_number|
    next if line.strip.empty?

    event = JSON.parse(line)
    location = "#{path}:#{line_number}"
    assert(event.is_a?(Hash), "#{location}: event must be an object")
    assert(event["schema_version"] == "0.0.1", "#{location}: unsupported schema_version")
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
    check_sensitive_values(event, location)
    count += 1
  rescue JSON::ParserError, ArgumentError => e
    fail_validation("#{location}: invalid event: #{e.message}")
  end
  count
end

def validate_evidence(path, feature_ids, failure_classes)
  evidence = read_json(path)
  assert(evidence["schema_version"] == "0.0.1", "#{path}: unsupported schema_version")
  assert(feature_ids.include?(evidence["feature"]), "#{path}: unknown feature")
  assert_sha(evidence.dig("artifact", "head_sha"), "#{path}: artifact.head_sha")
  assert_sha(evidence.dig("artifact", "tree"), "#{path}: artifact.tree")
  assert(RESULT_STATUSES.include?(evidence.dig("result", "status")), "#{path}: invalid result status")
  failure_class = evidence["failure_class"]
  assert(failure_class.nil? || failure_classes.include?(failure_class), "#{path}: invalid failure_class")
  assert(evidence.dig("environment", "host_class").is_a?(String), "#{path}: environment.host_class is required")
  assert(evidence.dig("execution", "commands").is_a?(Array), "#{path}: execution.commands must be an array")
  Time.iso8601(evidence.dig("execution", "started_at"))
  Time.iso8601(evidence.dig("execution", "finished_at"))
  check_sensitive_values(evidence, path)
  evidence
rescue ArgumentError => e
  fail_validation("#{path}: invalid timestamp: #{e.message}")
end

def validate_append_only(path)
  stdout, _stderr, status = Open3.capture3("git", "-C", ROOT, "merge-base", "HEAD", "origin/develop")
  return "not_checked" unless status.success?

  base = stdout.strip
  previous, _stderr, previous_status = Open3.capture3("git", "-C", ROOT, "show", "#{base}:.hid/events.jsonl")
  return "initial_log" unless previous_status.success?

  current = File.read(path)
  assert(current.start_with?(previous), "#{path}: committed base events were modified or removed")
  "preserved"
end

begin
  project_path = File.join(HID_ROOT, "project.yaml")
  project = validate_project(project_path)

  feature_paths = Dir[File.join(HID_ROOT, "features", "*.yaml")].sort
  assert(!feature_paths.empty?, "no HID feature manifests found")
  features = feature_paths.map { |path| validate_feature(path, project) }
  feature_ids = features.map { |feature| feature["id"] }

  template_path = File.join(HID_ROOT, "templates", "evidence.json")
  read_json(template_path)

  evidence_paths = Dir[File.join(HID_ROOT, "evidence", "*.json")].sort
  evidence_paths.each { |path| validate_evidence(path, feature_ids, project["failure_classes"]) }

  events_path = File.join(HID_ROOT, "events.jsonl")
  event_count = validate_events(events_path, project, feature_ids)
  append_only = validate_append_only(events_path)

  puts "HID validation: PASS"
  puts "features=#{feature_paths.length}"
  puts "evidence=#{evidence_paths.length}"
  puts "events=#{event_count}"
  puts "append_only=#{append_only}"
rescue HidValidationError => e
  warn "HID validation: FAIL"
  warn e.message
  exit 1
end
