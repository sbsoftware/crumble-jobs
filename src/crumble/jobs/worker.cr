require "log"

require "./queue_backend"

module Crumble
  module Jobs
    class Worker
      private class JobClassThrottleState
        property window_started_at : Time::Instant?
        property jobs_started_in_window : Int32

        def initialize
          @window_started_at = nil
          @jobs_started_in_window = 0
        end
      end

      private class JobClassState
        getter lock : Channel(Nil)
        getter throttle : JobClassThrottleState

        def initialize
          @lock = Channel(Nil).new(1)
          @lock.send(nil)
          @throttle = JobClassThrottleState.new
        end
      end

      def initialize(
        @queue : QueueBackend = Crumble::Jobs.queue,
        @poll_interval : Time::Span = 1.second,
        @logger : Log = Log.for("crumble.jobs.worker"),
        max_concurrency : Int32 = 1,
      )
        @max_concurrency = max_concurrency < 1 ? 1 : max_concurrency
        @semaphore = Channel(Nil).new(@max_concurrency)
        @max_concurrency.times { @semaphore.send(nil) }
        @job_class_states = {} of String => JobClassState
        @job_class_states_lock = Mutex.new
      end

      def run_once(wait : Time::Span? = nil) : Bool
        wait = @poll_interval if wait.nil?
        reservation = @queue.reserve(wait)
        return false unless reservation

        job_class_state = job_class_state_for(reservation.payload.job_class)
        job_class_state.lock.receive
        @semaphore.receive
        spawn do
          begin
            run_reservation(reservation, job_class_state)
          ensure
            job_class_state.lock.send(nil)
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

      private def run_reservation(reservation : Reservation, job_class_state : JobClassState) : Nil
        payload = reservation.payload
        throttle_job_execution(payload.job_class, job_class_state.throttle)
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

      private def job_class_state_for(job_class : String) : JobClassState
        @job_class_states_lock.synchronize do
          @job_class_states[job_class]? || begin
            state = JobClassState.new
            @job_class_states[job_class] = state
            state
          end
        end
      end

      private def throttle_job_execution(job_class : String, throttle : JobClassThrottleState) : Nil
        config = Crumble::Jobs.throttle_config_for(job_class)
        return unless config

        loop do
          now = Time.instant
          window_started_at = throttle.window_started_at

          if window_started_at.nil? || now - window_started_at >= config.timespan
            throttle.window_started_at = now
            throttle.jobs_started_in_window = 0
            window_started_at = now
          end

          if throttle.jobs_started_in_window < config.max_jobs
            throttle.jobs_started_in_window += 1
            return
          end

          wait_for = config.timespan - (now - window_started_at)
          sleep(wait_for) if wait_for > Time::Span.zero
        end
      end
    end
  end
end
