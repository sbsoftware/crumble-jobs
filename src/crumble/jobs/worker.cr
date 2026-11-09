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
        @shutdown_timeout : Time::Span = 30.seconds,
      )
        @max_concurrency = max_concurrency < 1 ? 1 : max_concurrency
        @semaphore = Channel(Nil).new(@max_concurrency)
        @max_concurrency.times { @semaphore.send(nil) }
        @active_reservations = {} of UInt64 => Reservation
        @active_reservations_lock = Mutex.new
        @reservation_sequence = 0_u64
        @stopping = false
      end

      def run_once(wait : Time::Span? = nil) : Bool
        return false if stopping?
        wait = @poll_interval if wait.nil?
        @semaphore.receive
        if stopping?
          @semaphore.send(nil)
          return false
        end
        reservation = @queue.reserve(wait)
        unless reservation
          @semaphore.send(nil)
          return false
        end

        unless reservation_id = track(reservation)
          reservation.release
          @semaphore.send(nil)
          return false
        end
        spawn do
          begin
            run_reservation(reservation)
          ensure
            untrack(reservation_id)
            @semaphore.send(nil)
          end
        end

        true
      end

      def start : Nil
        Crumble::Jobs.seed_periodic_jobs(@queue)
        install_signal_handlers
        until stopping?
          run_once(@poll_interval)
        end
        drain
      end

      def shutdown(timeout : Time::Span = @shutdown_timeout) : Nil
        request_stop
        drain(timeout)
      end

      private def install_signal_handlers : Nil
        Signal::TERM.trap { request_stop }
        Signal::INT.trap { request_stop }
      end

      private def drain(timeout : Time::Span = @shutdown_timeout) : Nil
        deadline = Time.instant + timeout
        until active_reservations.empty? || Time.instant >= deadline
          sleep 1.millisecond
        end
        active_reservations.each(&.release)
      end

      private def track(reservation : Reservation) : UInt64?
        @active_reservations_lock.synchronize do
          return nil if @stopping
          @reservation_sequence += 1
          @active_reservations[@reservation_sequence] = reservation
          @reservation_sequence
        end
      end

      private def untrack(reservation_id : UInt64) : Nil
        @active_reservations_lock.synchronize { @active_reservations.delete(reservation_id) }
      end

      private def active_reservations : Array(Reservation)
        @active_reservations_lock.synchronize { @active_reservations.values }
      end

      private def stopping? : Bool
        @active_reservations_lock.synchronize { @stopping }
      end

      private def request_stop : Nil
        @active_reservations_lock.synchronize { @stopping = true }
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
