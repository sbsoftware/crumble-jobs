require "json"

require "./value_codec"

module Crumble
  module Jobs
    struct JobPayload
      include JSON::Serializable

      getter id : String
      getter job_class : String
      getter args : Array(EncodedValue)
      getter enqueued_at : Int64
      @[JSON::Field(default: {} of String => Int32)]
      getter retry_counts : Hash(String, Int32)

      def initialize(
        @id : String,
        @job_class : String,
        @args : Array(EncodedValue),
        @enqueued_at : Int64,
        @retry_counts : Hash(String, Int32) = {} of String => Int32,
      )
      end

      def enqueued_time : Time
        Time.unix(@enqueued_at)
      end

      def retry_count_for(error_class : String) : Int32
        @retry_counts[error_class]? || 0
      end
    end
  end
end
