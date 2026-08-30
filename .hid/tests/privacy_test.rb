# frozen_string_literal: true

require_relative "test_helper"

class HidPrivacyTest < HidTestCase
  def test_api_key_field_is_privacy_violation
    assert privacy_findings({"api_key" => "value"}).any? { |finding| finding.level == "privacy_violation" }
  end

  def test_secret_assignment_in_note_is_privacy_violation
    findings = privacy_findings({"note" => "api_key=example-secret-value"})
    assert findings.any? { |finding| finding.kind == "secret_assignment" && finding.level == "privacy_violation" }
  end

  def test_password_in_arbitrary_string_is_privacy_violation
    findings = privacy_findings({"description" => "password=test-value"})
    assert findings.any? { |finding| finding.kind == "secret_assignment" && finding.level == "privacy_violation" }
  end

  def test_prompt_content_is_privacy_violation
    findings = privacy_findings({"prompt" => "implement X using bounded context"})
    assert findings.any? { |finding| finding.kind == "prohibited_payload" && finding.level == "privacy_violation" }
  end

  def test_model_response_content_is_privacy_violation
    findings = privacy_findings({"response" => "complete model output"})
    assert findings.any? { |finding| finding.kind == "prohibited_payload" && finding.level == "privacy_violation" }
  end

  def test_prompt_metadata_is_allowed
    assert_empty privacy_findings({"prompt_id" => "P123", "prompt_hash" => "abc123", "prompt_tokens" => 420})
  end

  def test_safe_api_key_status_is_allowed
    assert_empty privacy_findings({"api_key_status" => "not_configured"})
  end

  def test_compressed_ipv6_is_privacy_warning
    findings = privacy_findings({"note" => "2001:db8::1"})
    assert findings.any? { |finding| finding.kind == "ipv6" && finding.level == "privacy_warning" }
  end

  def test_full_ipv6_is_privacy_warning
    findings = privacy_findings({"note" => "2001:0db8:85a3:0000:0000:8a2e:0370:7334"})
    assert findings.any? { |finding| finding.kind == "ipv6" && finding.level == "privacy_warning" }
  end

  def test_ipv4_mapped_ipv6_is_privacy_warning
    findings = privacy_findings({"note" => "::ffff:192.0.2.128"})
    assert findings.any? { |finding| finding.kind == "ipv6" && finding.level == "privacy_warning" }
  end

  def test_ipv6_documentation_label_is_allowed
    assert_empty privacy_findings({"note" => "IPv6 documentation label without actual address"})
  end

  def test_local_path_is_privacy_warning
    assert privacy_findings({"note" => "/Users/person/project"}).any? { |finding| finding.level == "privacy_warning" }
  end
end
