require "log"

require "./queue_backend"

module Crumble
  module Jobs
    class Worker
      def initialize(
        @queue : QueueBackend = Crumble::Jobs.queue,
        @poll_interval : Time::Span = 1.second,
        @logger : Log = Log.for("crumble.jobs.worker"),
        max_concurrency : Int32 = 1,
      )
        @max_concurrency = max_concurrency < 1 ? 1 : max_concurrency
        @semaphore = Channel(Nil).new(@max_concurrency)
        @max_concurrency.times { @semaphore.send(nil) }
      end

      def run_once(wait : Time::Span? = nil) : Bool
        wait = @poll_interval if wait.nil?
        reservation = @queue.reserve(wait)
        return false unless reservation

        @semaphore.receive
        spawn do
          begin
            run_reservation(reservation)
          ensure
            @semaphore.send(nil)
          end
        end

        true
      end

      def start : Nil
        loop do
          run_once(@poll_interval)
        end
      end

      private def run_reservation(reservation : Reservation) : Nil
        payload = reservation.payload
        job = Crumble::Jobs.deserialize(payload)
        job.perform
        reservation.ack
      rescue error : UnknownJobError
        @logger.error { error.message }
        reservation.fail(error)
      rescue error
        if payload
          @logger.error(exception: error) { "Job failed: #{payload.job_class} (#{payload.id})" }
        else
          @logger.error(exception: error) { "Job failed before payload decode" }
        end
        reservation.fail(error)
      end
    end
  end
end
