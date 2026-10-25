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
        @semaphore.receive
        reservation = @queue.reserve(wait)
        unless reservation
          @semaphore.send(nil)
          return false
        end

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
        Crumble::Jobs.seed_periodic_jobs(@queue)
        loop do
          run_once(@poll_interval)
        end
      end

      private def run_reservation(reservation : Reservation) : Nil
        payload = reservation.payload
        job = nil
        job = Crumble::Jobs.deserialize(payload)
        job.perform
        schedule_next_periodic(job)
        reservation.ack
      rescue error : UnknownJobError
        @logger.error { error.message }
        reservation.fail(error)
      rescue error
        if payload && job
          if retry_counter_key = job.retry_counter_key_for(error)
            tries = payload.retry_count_for(retry_counter_key) + 1
            if retry_in = job.next_retry_in(error, tries)
              payload.retry_counts[retry_counter_key] = tries
              @queue.enqueue(payload, run_at: Time.utc + retry_in)
              reservation.ack
              return
            end
            @logger.error(exception: error) { "Job retries exhausted: #{payload.job_class} (#{payload.id})" }
          else
            @logger.error(exception: error) { "Job failed: #{payload.job_class} (#{payload.id})" }
          end
        else
          @logger.error(exception: error) { "Job failed before payload decode" }
        end
        schedule_next_periodic(job) if job
        reservation.fail(error)
      end

      private def schedule_next_periodic(job : Job) : Nil
        return unless interval = job.class.periodic_interval
        @queue.enqueue(job.to_payload, run_at: Time.utc + interval)
      end
    end
  end
end
