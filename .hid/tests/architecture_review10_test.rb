# frozen_string_literal: true

# Architecture Review #10 harness: all eleven passing cases are retained below.
# The two reproduced attacks now require writer rejection without ref movement.
require_relative "realistic_lifecycle_test"
class HidArchitectureReview10ProjectionTest < HidTestCase
  include OperationalFixture
  def report(id, input, expected, actual)
    assert actual, "#{id}: #{input}; expected #{expected}"
  end
  def test_review10_self_claim
    %w[architecture final_review].each do |gate|
      event = operational_event(gate)
      event['outcome'].delete('mandate')
      event['metadata'] = {'mandated' => true, 'authorized' => true, 'role' => 'architect'}
      error = assert_raises(Hid::ValidationError) { operational_projection([event]) }
      assert_match(/mandate missing or wrong/, error.message)
      report("self_claim_#{gate}", 'architect PASS; no mandate; authorized=true', 'reject', error.message)
    end
    project = state_project
    project['operational_policy']['mandates']['iamine-architecture']['features'] = ['OTHER-FEATURE-001']
    event = operational_event('final_review')
    event['metadata'] = {'mandated' => true, 'authorized' => true}
    error = assert_raises(Hid::ValidationError) { operational_projection([event], project: project) }
    report('external_mandate_scope', 'policy excludes feature; self-claimed authorization', 'reject', error.message)
  end
  def test_review10_legacy_pass_and_future_timestamp
    feature = operational_feature
    feature['gates'].each_value { |gate| gate['status'] = 'passed' }
    feature['state']['current'] = 'APPROVED FOR MERGE'
    events = nonhuman_events
    events.last['ts'] = '2099-01-01T00:00:00Z'
    failure = operational_event('final_review', result: 'fail')
    failure['ts'] = '2026-01-01T00:00:00Z'
    result = operational_projection(events + [failure], feature: feature)
    assert_equal 'failed', result.dig('gates', 'final_review', 'status')
    assert_equal 'CHANGES REQUIRED', result['state']
    assert_equal 'resolve_final_review', result['next_action']
    assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
    report('legacy_vs_latest_failure', 'legacy PASS; PASS ts2099 then ledger FAIL ts2026', 'CHANGES REQUIRED; no capsule', result.slice('state', 'next_action', 'human_gate_eligibility'))
    feature['gates'].each_value { |gate| gate['status'] = 'pending' }
    recovered = operational_projection(events + [failure, operational_event('final_review')], feature: feature)
    assert_equal 'passed', recovered.dig('gates', 'final_review', 'status')
    assert_equal 'request_human_gate', recovered['next_action']
    assert_equal 'READY', recovered.dig('human_gate_eligibility', 'status')
  end
  def test_review10_early_human_then_final
    first = nonhuman_events.take(4)
    capsule = capsule_event(first)
    early = human_event(capsule)
    result = operational_projection(first + [capsule, early, operational_event('final_review')])
    assert_equal 'blocked', result.dig('gates', 'human_merge', 'status')
    assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
    refute_equal 'APPROVED FOR MERGE', result['state']
    assert result['failures'].any? { |f| f.start_with?('APPROVAL_CAPSULE_NOT_READY') }
    report('early_human_then_final', 'human APPROVE before final PASS', 'cannot bootstrap approval', result.slice('state', 'next_action', 'failures'))
  end
  def test_review10_stale_and_foreign_review
    events = nonhuman_events.take(4)
    stale = operational_event('final_review', artifact: current_candidate(head: OTHER_HEAD, tree: OTHER_TREE))
    foreign = operational_event('final_review')
    foreign['feature'] = 'OTHER-FEATURE-001'
    foreign['outcome']['evidence']['feature'] = 'OTHER-FEATURE-001'
    project = state_project
    project['operational_policy']['mandates']['iamine-architecture']['features'] << 'OTHER-FEATURE-001'
    result = operational_projection(events + [stale, foreign], project: project)
    assert_equal 'pending', result.dig('gates', 'final_review', 'status')
    assert_equal 'request_final_review', result['next_action']
    assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
    report('stale_and_foreign_review', 'prior gates PASS; stale final + other feature final', 'final pending', result.slice('state', 'next_action', 'human_gate_eligibility'))
  end
  def custom_policy(required:)
    feature = operational_feature
    feature['gates']['security_review'] = {'required' => required, 'status' => 'passed'}
    project = state_project
    project['operational_policy']['gate_authorities']['security_review'] = {'kind' => 'review', 'authority_kind' => 'REVIEW_VERDICT', 'phase' => 'security_review', 'mandate' => 'iamine-architecture'}
    project['operational_policy']['mandates']['iamine-architecture']['gates'] << 'security_review'
    [feature, project]
  end
  def test_review10_custom_policy_requirement
    feature, project = custom_policy(required: false)
    project['state_requirements']['APPROVED FOR MERGE']['gates']['security_review'] = 'passed'
    events = nonhuman_events
    before = operational_projection(events, feature: feature, project: project)
    assert_equal true, before.dig('gates', 'security_review', 'required')
    assert_equal 'pending', before.dig('gates', 'security_review', 'status')
    assert_equal 'security_review', before.dig('human_gate_eligibility', 'missing_gate')
    request = operational_envelope('human_decision_requested').merge('capsule' => {'action' => 'merge', 'target_branch' => 'develop', 'prerequisite_events' => events.map { |e| e['id'] }})
    result = operational_projection(events + [request], feature: feature, project: project)
    assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
    refute_empty result['failures']
    report('custom_required_capsule', 'policy requires security_review; fact absent; capsule requested', 'NOT_READY', result.slice('state', 'next_action', 'failures'))
    complete = events.take(4) + [operational_event('security_review', project: project), operational_event('final_review')]
    assert_equal 'READY', operational_projection(complete, feature: feature, project: project).dig('human_gate_eligibility', 'status')
  end
  def test_review10_optional_custom_fact
    feature, project = custom_policy(required: false)
    event = operational_event('security_review', project: project)
    events = nonhuman_events.take(4) + [event, operational_event('final_review')]
    result = operational_projection(events, feature: feature, project: project)
    assert_equal false, result.dig('gates', 'security_review', 'required')
    assert_equal 'not_required', result.dig('gates', 'security_review', 'status')
    refute_includes result.dig('human_gate_eligibility', 'prerequisite_events'), event['id']
    report('optional_custom_fact', 'optional security_review PASS', 'no added authority', result.dig('gates', 'security_review'))
  end
  def test_review10_wrong_phase
    event = operational_event('final_review')
    event['outcome']['phase'] = 'architecture_checkpoint'
    event['outcome']['evidence']['kind'] = 'architecture_checkpoint'
    error = assert_raises(Hid::ValidationError) { operational_projection(nonhuman_events.take(4) + [event]) }
    assert_match(/wrong review\/validation phase/, error.message)
    report('wrong_phase', 'correct reviewer/artifact; final gate with checkpoint phase', 'reject', error.message)
  end
  def test_review10_evidence_self_claim
    events = nonhuman_events
    evidence = events[2]['outcome']['evidence']
    evidence['artifact'] = current_candidate(head: OTHER_HEAD, tree: OTHER_TREE)
    evidence['evidence_status'] = 'VALID'
    error = assert_raises(Hid::ValidationError) { operational_projection(events) }
    assert_match(/evidence feature\/artifact mismatch/, error.message)
    report('evidence_self_claim', 'validation current; evidence stale but marked VALID', 'reject', error.message)
    events[2] = operational_event('local_validation', artifact: current_candidate(head: OTHER_HEAD, tree: OTHER_TREE))
    result = operational_projection(events)
    assert_equal 'pending', result.dig('gates', 'local_validation', 'status')
    assert_equal 'run_local_validation', result['next_action']
    assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
  end
  def test_review10_reordered_manifest_required_qa_unknown
    feature = operational_feature
    feature['gates'] = feature['gates'].to_a.reverse.to_h
    feature['gates']['field_qa']['required'] = true
    assert_equal 'request_architecture_review', operational_projection([], feature: feature)['next_action']
    result = operational_projection(nonhuman_events, feature: feature)
    assert_equal 'run_field_qa', result['next_action']
    assert_equal 'pending', result.dig('gates', 'field_qa', 'status')
    complete = operational_projection(nonhuman_events(field_qa: true), feature: feature)
    assert_equal 'READY', complete.dig('human_gate_eligibility', 'status')
    feature['gates']['field_qa']['required'] = nil
    unknown = operational_projection(nonhuman_events(field_qa: true), feature: feature)
    assert_equal 'unknown', unknown.dig('gates', 'field_qa', 'status')
    assert_equal 'NOT_READY', unknown.dig('human_gate_eligibility', 'status')
    report('qa_and_unknown', 'reverse keys; required QA absent/pass/unknown', 'canonical order; pending/READY/NOT_READY', [result['next_action'], complete.dig('human_gate_eligibility', 'status'), unknown.dig('human_gate_eligibility', 'status')])
  end
  def test_review10_projection_purity
    feature, project, events = operational_feature, state_project, approved_events
    before = Marshal.dump([feature, project, events])
    result = operational_projection(events, feature: feature, project: project)
    assert_equal 'APPROVED FOR MERGE', result['state']
    assert_equal before, Marshal.dump([feature, project, events])
    report('projection_purity', 'calculate state/action from complete inputs', 'inputs unchanged', 'unchanged')
  end
end
class HidRealisticLifecycleTest
  def test_review10_union_bypass
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      before = subject_identity(root)
      nonhuman_events(artifact: candidate).take(4).each { |event| validator.append_control_event(event) }
      ledger = Hid::ControlLedger.new(root, ref: 'refs/heads/hid/control-plane', events_path: 'events.jsonl')
      control_head = ledger.head
      disguised = human_event(artifact: candidate, decision: 'denied')
      disguised['outcome'] = {'gate' => 'final_review', 'result' => 'pass'}
      error = assert_raises(Hid::ValidationError) { validator.append_control_event(disguised) }
      assert_match(/reserved or unknown event fields/, error.message)
      after = projection(validator)
      assert_equal control_head, ledger.head
      assert_equal before, subject_identity(root)
      assert_equal 'pending', after.dig('gates', 'final_review', 'status')
      assert_equal 'NOT_READY', after.dig('human_gate_eligibility', 'status')
      refute_equal 'APPROVED FOR MERGE', after['state']
    end
  end

  def test_review10_fabricated_gates
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      before = subject_identity(root)
      ledger = Hid::ControlLedger.new(root, ref: 'refs/heads/hid/control-plane', events_path: 'events.jsonl')
      events = %w[architecture implementation local_validation architecture_checkpoint final_review].map do |gate|
        event = human_event(artifact: candidate, decision: 'denied')
        event['outcome'] = {'gate' => gate, 'result' => 'pass'}
        assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
        assert_nil ledger.head
        assert_equal before, subject_identity(root)
        assert_equal 'pending', projection(validator).dig('gates', gate, 'status')
        event
      end
      # Preserve the rest of the original attack even after each rejected write.
      capsule = operational_envelope('human_decision_requested', artifact: candidate).merge(
        'capsule' => {'action' => 'merge', 'target_branch' => 'develop', 'prerequisite_events' => events.map { |e| e['id'] }}
      )
      assert_raises(Hid::ValidationError) { validator.append_control_event(capsule) }
      assert_raises(Hid::ValidationError) { validator.append_control_event(human_event(capsule, artifact: candidate)) }
      result = projection(validator)
      assert_equal before, subject_identity(root)
      assert_nil ledger.head
      assert_empty ledger.entries
      assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
      refute_equal 'APPROVED FOR MERGE', result['state']
      assert_raises(Hid::ValidationError) { operational_projection(events + [capsule, human_event(capsule, artifact: candidate)], current: candidate, git: git) }
    end
  end

  def test_review10_negative_real_path
    with_real_workspace do |root, validator, git|
      candidate = git.capture
      initial = subject_identity(root)
      ledger = Hid::ControlLedger.new(root, ref: 'refs/heads/hid/control-plane', events_path: 'events.jsonl')
      states = [projection(validator).slice('state', 'next_action')]
      events = nonhuman_events(artifact: candidate).take(4)
      events.each do |event|
        validator.append_control_event(event)
        assert_equal initial, subject_identity(root)
        states << projection(validator).slice('state', 'next_action')
      end
      negative = operational_event('final_review', result: 'fail', artifact: candidate)
      validator.append_control_event(negative)
      events << negative
      result = projection(validator)
      states << result.slice('state', 'next_action')
      assert_equal 'CHANGES REQUIRED', result['state']
      assert_equal 'resolve_final_review', result['next_action']
      assert_equal 'NOT_READY', result.dig('human_gate_eligibility', 'status')
      assert_equal initial, subject_identity(root)
      before_head = ledger.head
      before_refs = git!(root, 'for-each-ref', '--format=%(refname) %(objectname)')
      capsule = capsule_event(events, artifact: candidate, git: git)
      self_claim = operational_event('final_review', artifact: candidate)
      self_claim['outcome']['mandate'] = 'self-authorized'
      self_claim['metadata'] = {'authorized' => true, 'mandated' => true}
      false_git = operational_event('final_review', artifact: candidate)
      false_git['artifact']['tree'] = TREE
      false_git['outcome']['evidence']['artifact']['tree'] = TREE
      false_git['outcome']['evidence']['evidence_status'] = 'VALID'
      [capsule, human_event(capsule, artifact: candidate), self_claim, false_git].each do |event|
        error = assert_raises(Hid::ValidationError) { validator.append_control_event(event) }
        assert_equal before_head, ledger.head
        assert_equal initial, subject_identity(root)
      end
      2.times { projection(validator) }
      assert_equal before_refs, git!(root, 'for-each-ref', '--format=%(refname) %(objectname)')
      assert_equal initial, subject_identity(root)
      assert_equal before_head, ledger.head
      persisted = ledger.entries.map { |entry| JSON.parse(entry.json)['event'] }
      refute_includes persisted, 'human_decision_requested'
      refute_includes persisted, 'human_authorization'
      refute_includes persisted, 'merged'
      assert_equal 5, persisted.length
    end
  end
end
