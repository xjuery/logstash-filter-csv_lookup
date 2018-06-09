# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'csv'

# This csv_lookup filter will replace the contents of the default
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::CSV_Lookup < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   csv_lookup {
  #     message => "My message..."
  #   }
  # }
  #
  config_name "csv_lookup"

  # CSV parameters
  config :path, :validate => :string, :required => true
  # Supports field reference syntax eg: `fields => ["field1", "field2"]`.
  config :fields, :validate => :array, :required => true
  config :primarykey, :validate => :string, :required => true
  config :quote_char, :validate => :string, :required => false, :default => '"'
  config :col_sep, :validate => :string, :required => false, :default => ','
  config :headers, :validate => :boolean, :required => false, :default => false


  config :event_foreignkey, :validate => :string, :required => true
  config :join_mode, :validate => :string, :required => false, :default => "innerjoin"


  attr_reader :datacache

  public
  def register
    @datacache = Hash.new
    CSV.foreach(@path, quote_char: @quote_char, col_sep: @col_sep, row_sep: :auto, headers: @headers) do |row|
      ligne = Hash.new
      pkindex = 0

      row.each_with_index do |item, index|
        ligne[@fields[index]] = item
        if @fields[index] == @primarykey
          pkindex = index
        end
      end

      datacache[row[pkindex]] = ligne
    end # CSV.foreach

  end # def register

  public
  def filter(event)
    csvrow = @datacache[event.get(@event_foreignkey)]

    if csvrow == nil
      if @join_mode != "leftjoin"
        #i.e. join_mode == innerjoin or by default drop event 
        event.cancel
      end

    else
      csvrow.each {|key, value|
        event.set(key, value)
      }
    end

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::CSV_Lookup
