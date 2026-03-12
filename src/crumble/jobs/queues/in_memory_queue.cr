require "../queue_backend"
require "../throttle_state"

module Crumble
  module Jobs
    class InMemoryJobClassState
      getter execution_lock : Channel(Nil)
      getter throttle : ThrottleWindowState

      def initialize
        @execution_lock = Channel(Nil).new(1)
        @execution_lock.send(nil)
        @throttle = ThrottleWindowState.new
      end
    end

    class InMemoryReservation < Reservation
      def initialize(payload : JobPayload, @execution_lock : Channel(Nil))
        super(payload)
        @released = false
      end

      def ack : Nil
        release_execution_lock
      end

      def fail(error : Exception? = nil) : Nil
        release_execution_lock
      end

      private def release_execution_lock : Nil
        return if @released
        @released = true
        @execution_lock.send(nil)
      end
    end

    class InMemoryQueue < QueueBackend
      DEFAULT_CAPACITY = 100

      def initialize(@capacity : Int32 = DEFAULT_CAPACITY)
        @channel = Channel(JobPayload).new(@capacity)
        @job_class_states = {} of String => InMemoryJobClassState
        @job_class_states_lock = Mutex.new
      end

      def enqueue(payload : JobPayload) : Nil
        @channel.send(payload)
      end

      def reserve(wait : Time::Span? = nil) : Reservation?
        if wait
          select
          when payload = @channel.receive
            reservation_for(payload)
          when timeout(wait)
            nil
          end
        else
          payload = @channel.receive
          reservation_for(payload)
        end
      end

      private def reservation_for(payload : JobPayload) : InMemoryReservation
        job_class_state = job_class_state_for(payload.job_class)
        job_class_state.execution_lock.receive
        begin
          throttle_job_execution(payload.job_class, job_class_state)
          InMemoryReservation.new(payload, job_class_state.execution_lock)
        rescue error
          job_class_state.execution_lock.send(nil)
          raise error
        end
      end

      private def job_class_state_for(job_class : String) : InMemoryJobClassState
        @job_class_states_lock.synchronize do
          @job_class_states[job_class]? || begin
            state = InMemoryJobClassState.new
            @job_class_states[job_class] = state
            state
          end
        end
      end

      private def throttle_job_execution(job_class : String, job_class_state : InMemoryJobClassState) : Nil
        config = Crumble::Jobs.throttle_config_for(job_class)
        return unless config

        timespan_milliseconds = config.timespan.total_milliseconds.to_i64
        loop do
          now = unix_milliseconds
          window_started_at = job_class_state.throttle.window_started_at_unix_ms

          # Keep a fixed execution window per class that starts with the first job in a burst.
          if window_started_at.nil? || now - window_started_at >= timespan_milliseconds
            job_class_state.throttle.window_started_at_unix_ms = now
            job_class_state.throttle.jobs_started_in_window = 0
            window_started_at = now
          end

          if job_class_state.throttle.jobs_started_in_window < config.max_jobs
            job_class_state.throttle.jobs_started_in_window += 1
            return
          end

          wait_for = timespan_milliseconds - (now - window_started_at)
          sleep(wait_for.milliseconds) if wait_for > 0
        end
      end

      private def unix_milliseconds : Int64
        now = Time.utc
        now.to_unix * 1000 + now.nanosecond // 1_000_000
      end
    end
  end
end
