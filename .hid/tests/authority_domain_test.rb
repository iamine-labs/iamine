# frozen_string_literal: true

require_relative "realistic_lifecycle_test"

class HidAuthorityDomainTest < HidTestCase
  include OperationalFixture

  def test_human_payload_cannot_cross_any_nonhuman_domain_even_without_writer
    %w[approved denied].each do |decision|
      %w[architecture implementation local_validation architecture_checkpoint field_qa final_review security_review merged post_merge_validation closure].each do |gate|
        event = human_event({"id" => "HID-EVENT-0099"}, decision: decision)
        event["outcome"] = {"gate" => gate, "result" => "pass"}
        assert_raises(Hid::ValidationError, "#{decision}/#{gate}") { operational_projection([event]) }
      end
    end
  end

  def test_minimum_cross_domain_matrix
    events = [human_event(decision: "denied"), operational_event("final_review"),
              operational_event("local_validation"), operational_event("final_review")]
    events[0]["authorization"]["gate"] = "final_review"
    events[1]["outcome"]["gate"] = "human_merge"
    events[2]["outcome"]["gate"] = "final_review"
    events[3]["outcome"]["gate"] = "local_validation"
    events.each { |event| assert_raises(Hid::ValidationError) { operational_projection([event]) } }
  end

  def test_policy_cannot_weaken_constitutional_authority_domains
    %w[architecture final_review local_validation human_merge].each do |gate|
      project = Marshal.load(Marshal.dump(state_project))
      rule = gate == "human_merge" ? project["human_gates"][gate] : project["operational_policy"]["gate_authorities"][gate]
      rule["authority_kind"] = "QA_RESULT"
      result = operational_projection([], project: project)
      assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
      assert result["failures"].any? { |failure| failure.start_with?("CONSTITUTIONAL_POLICY_VIOLATION") }
    end
  end

  def test_required_gate_without_resolvable_domain_fails_closed
    project = Marshal.load(Marshal.dump(state_project))
    project["operational_policy"]["gate_authorities"]["final_review"].delete("authority_kind")
    result = operational_projection([], project: project)
    assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
    assert_equal "policy_incomplete", result["next_action"]
  end

  def test_valid_review_and_validation_do_not_satisfy_other_gates
    %w[architecture local_validation final_review].each do |gate|
      result = operational_projection([operational_event(gate)])
      assert_equal "passed", result.dig("gates", gate, "status")
      %w[architecture local_validation final_review human_merge].reject { |other| other == gate }.each do |other|
        assert_equal "pending", result.dig("gates", other, "status")
      end
    end
  end

  def test_valid_human_approval_and_denial_preserve_review_results
    events = approved_events
    result = operational_projection(events)
    assert_equal "APPROVED FOR MERGE", result["state"]
    assert_equal "passed", result.dig("gates", "human_merge", "status")
    denied = operational_projection(events + [human_event(decision: "denied")])
    assert_equal "failed", denied.dig("gates", "human_merge", "status")
    assert_equal result.dig("gates", "final_review"), denied.dig("gates", "final_review")
  end

  def test_capsule_cannot_transport_outcome_or_authorization
    %w[outcome authorization].each do |field|
      event = capsule_event(nonhuman_events)
      event[field] = {"gate" => "final_review", "result" => "pass"}
      assert_raises(Hid::ValidationError) { operational_projection(nonhuman_events + [event]) }
    end
  end

  def test_typed_objects_are_not_accepted_as_raw_events
    fact = Hid::OperationalFacts.new(state_project).build(operational_event("final_review"))
    assert_raises(Hid::ValidationError) { operational_projection([fact]) }
  end
end

class HidRealisticLifecycleTest
  def test_domain_writer_rejects_human_outcomes_before_any_ref_movement
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      before = subject_identity(root)
      refs = git!(root, "for-each-ref", "--format=%(refname) %(objectname)")
      ledger = Hid::ControlLedger.new(root, ref: "refs/heads/hid/control-plane", events_path: "events.jsonl")
      %w[approved denied].each do |decision|
        %w[final_review local_validation field_qa].each do |gate|
          event = human_event({"id" => "HID-EVENT-0099"}, artifact: candidate, decision: decision)
          event["outcome"] = {"gate" => gate, "result" => "pass"}
          assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
          assert_nil ledger.head
          assert_equal before, subject_identity(root)
          assert_equal refs, git!(root, "for-each-ref", "--format=%(refname) %(objectname)")
        end
      end
    end
  end

  def test_domain_negative_final_review_cannot_be_neutralized_by_human
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      before = subject_identity(root)
      events = nonhuman_events(artifact: candidate)
      events.each { |event| validator.append_control_event(event) }
      capsule = capsule_event(events, artifact: candidate, git: git)
      validator.append_control_event(capsule)
      validator.append_control_event(operational_event("final_review", result: "fail", artifact: candidate))
      ledger = Hid::ControlLedger.new(root, ref: "refs/heads/hid/control-plane", events_path: "events.jsonl")
      control_head = ledger.head
      assert_raises(Hid::ValidationError) { validator.append_control_event(human_event(capsule, artifact: candidate)) }
      assert_equal control_head, ledger.head
      validator.append_control_event(human_event(artifact: candidate, decision: "denied"))
      result = projection(validator)
      assert_equal "failed", result.dig("gates", "final_review", "status")
      assert_equal "CHANGES REQUIRED", result["state"]
      assert_equal "resolve_final_review", result["next_action"]
      assert_equal "NOT_READY", result.dig("human_gate_eligibility", "status")
      refute_includes ledger.entries.map { |entry| JSON.parse(entry.json)["event"] }, "merged"
      assert_equal before, subject_identity(root)
    end
  end
end
