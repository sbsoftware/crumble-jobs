require "./job_payload"

module Crumble
  module Jobs
    abstract class Reservation
      getter payload : JobPayload

      def initialize(@payload : JobPayload)
      end

      abstract def ack : Nil
      abstract def fail(error : Exception? = nil) : Nil
    end

    abstract class QueueBackend
      abstract def enqueue(payload : JobPayload, run_at : Time = Time.utc) : Nil
      abstract def reserve(wait : Time::Span? = nil) : Reservation?
    end
  end
end
