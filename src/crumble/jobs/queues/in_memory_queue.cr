require "../queue_backend"
require "../throttle_state"

module Crumble
  module Jobs
    struct InMemoryDelayedPayload
      getter run_at_unix_ms : Int64
      getter sequence : Int64
      getter payload : JobPayload

      def initialize(@run_at_unix_ms : Int64, @sequence : Int64, @payload : JobPayload)
      end
    end

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
        @delayed_payloads = [] of InMemoryDelayedPayload
        @delayed_payloads_lock = Mutex.new
        @delayed_sequence = 0_i64
      end

      def enqueue(payload : JobPayload) : Nil
        @channel.send(payload)
      end

      def requeue_at(payload : JobPayload, run_at : Time) : Nil
        run_at_unix_ms = run_at.to_unix_ms
        return @channel.send(payload) if run_at_unix_ms <= unix_milliseconds

        @delayed_payloads_lock.synchronize do
          @delayed_sequence += 1
          @delayed_payloads << InMemoryDelayedPayload.new(run_at_unix_ms, @delayed_sequence, payload)
        end
      end

      def reserve(wait : Time::Span? = nil) : Reservation?
        deadline = wait ? Time.instant + wait : nil

        loop do
          if payload = due_delayed_payload
            reservation = reservation_for(payload)
            return reservation if reservation
            next
          end

          wait_for_receive = wait_for_next_receive(deadline)
          return nil if wait_for_receive == Time::Span.zero

          unless wait_for_receive
            payload = @channel.receive
            reservation = reservation_for(payload)
            return reservation if reservation
            next
          end

          select
          when payload = @channel.receive
            reservation = reservation_for(payload)
            return reservation if reservation
          when timeout(wait_for_receive)
          end
        end
      end

      private def reservation_for(payload : JobPayload) : InMemoryReservation?
        job_class_state = job_class_state_for(payload.job_class)
        job_class_state.execution_lock.receive
        begin
          if run_at_unix_ms = throttle_requeue_at(payload.job_class, job_class_state)
            requeue_at(payload, Time.unix_ms(run_at_unix_ms))
            job_class_state.execution_lock.send(nil)
            return nil
          end

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

      private def throttle_requeue_at(job_class : String, job_class_state : InMemoryJobClassState) : Int64?
        config = Crumble::Jobs.throttle_config_for(job_class)
        return nil unless config

        timespan_milliseconds = config.timespan.total_milliseconds.to_i64
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
          return nil
        end

        window_started_at + timespan_milliseconds
      end

      private def due_delayed_payload : JobPayload?
        now = unix_milliseconds
        @delayed_payloads_lock.synchronize do
          selected = nil

          @delayed_payloads.each_with_index do |entry, index|
            next if entry.run_at_unix_ms > now

            if selected.nil? || entry.run_at_unix_ms < @delayed_payloads[selected.not_nil!].run_at_unix_ms || (entry.run_at_unix_ms == @delayed_payloads[selected.not_nil!].run_at_unix_ms && entry.sequence < @delayed_payloads[selected.not_nil!].sequence)
              selected = index
            end
          end

          return nil unless selected

          @delayed_payloads.delete_at(selected).payload
        end
      end

      private def wait_for_next_receive(deadline : Time::Instant?) : Time::Span?
        delayed_wait = wait_for_next_delayed_payload
        return delayed_wait unless deadline

        remaining = deadline - Time.instant
        return Time::Span.zero if remaining <= Time::Span.zero
        return remaining unless delayed_wait
        remaining < delayed_wait ? remaining : delayed_wait
      end

      private def wait_for_next_delayed_payload : Time::Span?
        next_run_at_unix_ms = nil
        @delayed_payloads_lock.synchronize do
          @delayed_payloads.each do |entry|
            next_run_at_unix_ms = entry.run_at_unix_ms if next_run_at_unix_ms.nil? || entry.run_at_unix_ms < next_run_at_unix_ms
          end
        end

        return nil unless next_run_at_unix_ms
        wait_for = next_run_at_unix_ms - unix_milliseconds
        wait_for <= 0 ? Time::Span.zero : wait_for.milliseconds
      end

      private def unix_milliseconds : Int64
        now = Time.utc
        now.to_unix * 1000 + now.nanosecond // 1_000_000
      end
    end
  end
end
