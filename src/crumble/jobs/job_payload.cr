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

      def initialize(
        @id : String,
        @job_class : String,
        @args : Array(EncodedValue),
        @enqueued_at : Int64,
      )
      end

      def enqueued_time : Time
        Time.unix(@enqueued_at)
      end
    end
  end
end
