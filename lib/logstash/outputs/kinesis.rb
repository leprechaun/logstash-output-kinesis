# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/buffer"
require "digest/sha2"

# Push events to an Amazon Web Services Kinesis stream.
#
# Amazon Kinesis is a fully managed, cloud-based service for real-time data processing
# over large, distributed data streams.
#
# To use this plugin, you *must*:
#
#  * Have an AWS account
#  * Setup an Kinesis stream
#  * Create an identify that has access to publish messages to the stream.
#
# The "consumer" identity must have the following permissions on the strem:
#
#  * kinesis:PutRecords
#
# Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
# A sample policy is as follows:
# [source,ruby]
#      {
#        "Statement": [
#          {
#            "Sid": "Stmt1347986764948",
#            "Action": [
#                "kinesis:PutRecords",
#            ],
#            "Effect": "Allow",
#            "Resource": [
#              "arn:aws:kinesis:us-east-1:111122223333:stream/Logstash"
#            ]
#          }
#        ]
#      }
#
# See http://aws.amazon.com/iam/ for more details on setting up AWS identities.
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    kinesis {
#      access_key_id => "crazy_key"                (required)
#      secret_access_key => "monkey_access_key"    (required)
#      region => "eu-west-1"                       (required)
#      stream_name => "my_stream"                  (required)
#      event_partition_keys => ["message","@uuid"] (optional)
#      batch_events => 100                         (optional, batch size)
#      batch_timeout => 5                          (optional)
#    }
#
class LogStash::Outputs::Kinesis < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig
  include Stud::Buffer

  config_name "kinesis"
  milestone 1

  # Name of Kinesis stream to push messages into. Note that this is just the name of the stream, not the URL or ARN.
  config :stream_name, :validate => :string, :required => true

  # Name of the field in an event that contains the partition key for kinesis
  config :event_partition_keys, :validate => :array, :default => ["message", "@uuid"]

  # Set to true if you want send messages to Kinesis in batches with `put_records`
  # from the amazon sdk
  config :batch, :validate => :boolean, :default => true

  # If `batch` is set to true, the number of events we queue up for a `put_records`.
  config :batch_events, :validate => :number, :default => 100

  # If `batch` is set to true, the maximum amount of time between `put_records` commands when there are pending events to flush.
  config :batch_timeout, :validate => :number, :default => 5

  public
  def aws_service_endpoint(region)
    return {
        :kinesis_endpoint => "kinesis.#{region}.amazonaws.com"
    }
  end

  public 
  def register
    require "aws-sdk-resources"

    @kinesis = Aws::Kinesis::Client.new(
      region: @region,
      access_key_id: @access_key_id,
      secret_access_key: @secret_access_key
    )

    if @batch
      if @batch_events > 500
        raise RuntimeError.new(
          "AWS only allows a batch_events parameter of 10 or less"
        )
      elsif @batch_events <= 1
        raise RuntimeError.new(
          "batch_events parameter must be greater than 1 (or its not a batch)"
        )
      end
      buffer_initialize(
        :max_items => @batch_events,
        :max_interval => @batch_timeout,
        :logger => @logger
      )
    else
      raise NotImplementedError, 'Currently only batch write is supported'
    end
  end # def register

  public
  def receive(event)
    if @batch
      partition_key = ""

      @event_partition_keys.each do |partition_key_name|
        if not event[partition_key_name].nil? and event[partition_key_name].to_s.length > 0
          @logger.info("Found field named #{partition_key_name}")
          partition_key = event[partition_key_name].to_s
          break
        end
        @logger.info("No field named #{partition_key_name}")
      end

      buffer_receive(
        {
          data: event.to_json,
          partition_key: partition_key
        }
      )
    else
      raise NotImplementedError, 'Currently only batch write is supported'
    end
  end # def receive

  # Return a list of events that failed when writing to kinesis
  def get_failed_records(responses, events)
    # Iterate over response records
    response_record_index = 0
    failed_events = []

    # Check each page in the response
    if responses.class.name == "Array"
      responses.each do |response_page|
        # Check each record in each page
        response_page.data.records.each do |response_record|
          # Collect all failed records
          if not response_record.error_code.nil?
            @logger.info("put_records: #{response_record.error_message} (#{response_record.error_code})")
            failed_events.push(events[response_record_index])
          end
          response_record_index += 1
        end
      end
    end

    return failed_events
  end

  # called from Stud::Buffer#buffer_flush when there are events to flush
  def flush(events, teardown=false)
    # Initial backoff time
    backoff = 0.01

    # A retry loop
    loop do
      responses = @kinesis.put_records(
        records: events,
        stream_name: @stream_name
      )

      failed_events = get_failed_records(responses, events)

      # Retry if failed events if any
      break if failed_events.count == 0

      @logger.warn("Failed #{failed_events.count} records. Retrying in #{backoff}.")
      events = failed_events

      # Exponential + random backoff
      sleep(backoff)
      backoff *= 1.95 + rand() / 10
    end
  end

  public
  def teardown
    buffer_flush(:final => true)
    finished
  end # def teardown
end
