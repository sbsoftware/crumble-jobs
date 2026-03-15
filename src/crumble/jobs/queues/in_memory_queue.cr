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
      getter throttle : ThrottleWindowState
      getter lock : Mutex

      def initialize
        @throttle = ThrottleWindowState.new
        @lock = Mutex.new
      end
    end

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
        @job_class_states = {} of String => InMemoryJobClassState
        @job_class_states_lock = Mutex.new
        @delayed_payloads = [] of InMemoryDelayedPayload
        @delayed_payloads_lock = Mutex.new
        @delayed_sequence = 0_i64
      end

      def enqueue(payload : JobPayload, run_at : Time = Time.utc) : Nil
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
        if run_at_unix_ms = throttle_requeue_at(payload.job_class)
          enqueue(payload, run_at: Time.unix_ms(run_at_unix_ms))
          return nil
        end

        InMemoryReservation.new(payload)
      end

      private def throttle_requeue_at(job_class : String) : Int64?
        config = Crumble::Jobs.throttle_config_for(job_class)
        return nil unless config

        timespan_milliseconds = config.timespan.total_milliseconds.to_i64
        job_class_state = job_class_state_for(job_class)
        job_class_state.lock.synchronize do
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

      private def due_delayed_payload : JobPayload?
        now = unix_milliseconds
        @delayed_payloads_lock.synchronize do
          selected_index = nil
          selected = nil

          @delayed_payloads.each_with_index do |entry, index|
            next if entry.run_at_unix_ms > now
            if selected.nil? || entry.run_at_unix_ms < selected.not_nil!.run_at_unix_ms || (entry.run_at_unix_ms == selected.not_nil!.run_at_unix_ms && entry.sequence < selected.not_nil!.sequence)
              selected = entry
              selected_index = index
            end
          end

          return nil unless selected_index

          @delayed_payloads.delete_at(selected_index).payload
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
