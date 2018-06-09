# encoding: utf-8
require 'spec_helper'
require "logstash/filters/csv_lookup"

describe LogStash::Filters::CSV_Lookup do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        csv_lookup {
          message => "Hello World"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject.get("message")).to eq('Hello World')
    end
  end
end
