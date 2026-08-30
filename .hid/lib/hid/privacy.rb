# frozen_string_literal: true

require "yaml"

module Hid
  PrivacyFinding = Struct.new(:level, :kind, :source, :path)

  class PrivacyPolicy
    EMAIL = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i
    IPV4 = /\b(?:\d{1,3}\.){3}\d{1,3}\b/
    IPV6 = /\b(?:[0-9a-f]{1,4}:){2,7}[0-9a-f]{0,4}\b/i
    POSIX_LOCAL_PATH = %r{/(?:Users|home)/[^/\s]+(?:/[^\s]*)?}
    WINDOWS_LOCAL_PATH = /\b[A-Za-z]:\\[^\s]+/
    PRIVATE_KEY = /-----BEGIN [A-Z ]*PRIVATE KEY-----/
    BEARER = /\bBearer\s+[A-Za-z0-9._~+\/-]+=*/i
    TOKEN_VALUE = /\b(?:sk|ghp|github_pat|xox[baprs])[-_][A-Za-z0-9_-]{12,}\b/i
    CREDENTIAL_URL = %r{https?://[^/\s:@]+:[^@\s]+@}i
    SECRET_QUERY = /[?&](?:api[_-]?key|access[_-]?token|refresh[_-]?token|secret|password)=[^&#\s]+/i

    def self.load(path)
      data = YAML.safe_load(File.read(path), permitted_classes: [], aliases: false)
      new(data)
    end

    def initialize(data)
      @never_keys = Array(data.dig("NEVER_STORE", "keys")).map { |key| normalize(key) }
      @never_payload_keys = Array(data.dig("NEVER_STORE", "payload_keys")).map { |key| normalize(key) }
      @redact_key_names = Array(data.dig("REDACT", "keys")).map { |key| normalize(key) }
    end

    def findings(value, source)
      results = []
      each_leaf(value) do |path, leaf|
        key = normalize(path.last)
        results.concat(key_findings(key, leaf, source, path))
        results.concat(value_findings(leaf, source, path)) if leaf.is_a?(String)
      end
      results.uniq { |finding| [finding.level, finding.kind, finding.source, finding.path] }
    end

    private

    def normalize(value)
      value.to_s.downcase.tr("-", "_")
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

    def key_findings(key, leaf, source, path)
      return [] if leaf.nil? || leaf == false || leaf == "" || leaf == "not_measured"
      return [finding("privacy_violation", "never_store_key", source, path)] if @never_keys.include?(key)
      return [finding("privacy_violation", "prohibited_payload", source, path)] if @never_payload_keys.include?(key)
      return [finding("privacy_warning", "redact_key", source, path)] if @redact_key_names.include?(key)

      []
    end

    def value_findings(value, source, path)
      violations = {
        "private_key" => PRIVATE_KEY,
        "bearer_token" => BEARER,
        "token_value" => TOKEN_VALUE,
        "credential_url" => CREDENTIAL_URL,
        "secret_query" => SECRET_QUERY
      }
      warnings = {
        "email" => EMAIL,
        "ipv4" => IPV4,
        "ipv6" => IPV6,
        "local_posix_path" => POSIX_LOCAL_PATH,
        "local_windows_path" => WINDOWS_LOCAL_PATH
      }

      results = []
      violations.each do |kind, pattern|
        results << finding("privacy_violation", kind, source, path) if pattern.match?(value)
      end
      warnings.each do |kind, pattern|
        results << finding("privacy_warning", kind, source, path) if pattern.match?(value)
      end
      results
    end

    def finding(level, kind, source, path)
      PrivacyFinding.new(level, kind, source, path.join("."))
    end
  end
end
