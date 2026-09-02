#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/hid/validator"

root = File.expand_path("../..", __dir__)

begin
  report = Hid::Validator.new(root).run
  puts "HID validation: PASS"
  puts "features=#{report['features']}"
  puts "evidence=#{report['evidence']}"
  puts "events=#{report['events']}"
  puts "baseline_events=#{report['baseline_events']}"
  puts "control_events=#{report['control_events']}"
  puts "control_ledger=#{JSON.generate(report['control_ledger'])}"
  puts "current=#{JSON.generate(report['current'])}"
  report["evidence_statuses"].sort.each { |id, status| puts "evidence_status=#{id}:#{status}" }
  puts "append_only=#{report['append_only']}"
  report["next_actions"].sort.each { |id, action| puts "next_action=#{id}:#{action}" }
  report["warnings"].each { |warning| warn "HID warning: #{warning}" }
rescue Hid::ValidationError, Hid::GitUnavailable => e
  warn "HID validation: FAIL"
  warn e.message
  exit 1
end
