require "../queue_backend"

module Crumble
  module Jobs
    class InMemoryReservation < Reservation
      def ack : Nil
      end

      def fail(error : Exception? = nil) : Nil
      end
    end

    class InMemoryQueue < QueueBackend
      DEFAULT_CAPACITY = 100

      def initialize(@capacity : Int32 = DEFAULT_CAPACITY)
        @channel = Channel(JobPayload).new(@capacity)
      end

      def enqueue(payload : JobPayload) : Nil
        @channel.send(payload)
      end

      def reserve(wait : Time::Span? = nil) : Reservation?
        if wait
          select
          when payload = @channel.receive
            InMemoryReservation.new(payload)
          when timeout(wait)
            nil
          end
        else
          payload = @channel.receive
          InMemoryReservation.new(payload)
        end
      end
    end
  end
end
