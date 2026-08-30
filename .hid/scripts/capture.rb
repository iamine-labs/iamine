#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require_relative "../lib/hid/git_facts"

root = File.expand_path("../..", __dir__)

begin
  puts JSON.pretty_generate(Hid::GitFacts.new(root).capture)
rescue Hid::GitUnavailable => e
  warn "HID capture: UNKNOWN"
  warn e.message
  exit 1
end
