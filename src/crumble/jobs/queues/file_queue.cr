require "../queue_backend"
require "../throttle_state"
require "digest/sha1"

module Crumble
  module Jobs
    class FileReservation < Reservation
      def initialize(payload : JobPayload, @path : String, @failed_dir : String, @execution_lock : File)
        super(payload)
        @released = false
      end

      def ack : Nil
        File.delete(@path) if File.exists?(@path)
      rescue
      ensure
        release_execution_lock
      end

      def fail(error : Exception? = nil) : Nil
        failed_path = File.join(@failed_dir, File.basename(@path))

        begin
          File.rename(@path, failed_path) if File.exists?(@path)
        rescue
        end

        return unless error

        File.write("#{failed_path}.error", error.message)
      rescue
      ensure
        release_execution_lock
      end

      private def release_execution_lock : Nil
        return if @released
        @released = true
        begin
          @execution_lock.flock_unlock
        rescue
        end

        begin
          @execution_lock.close
        rescue
        end
      end
    end

    class FileQueue < QueueBackend
      DEFAULT_POLL_INTERVAL = 1.second

      def initialize(@root : String, @poll_interval : Time::Span = DEFAULT_POLL_INTERVAL)
        @ready_dir = File.join(@root, "ready")
        @processing_dir = File.join(@root, "processing")
        @failed_dir = File.join(@root, "failed")
        @tmp_dir = File.join(@root, "tmp")
        @throttle_dir = File.join(@root, "throttle")
        ensure_dirs
      end

      def enqueue(payload : JobPayload) : Nil
        filename = "#{timestamp_prefix}-#{payload.id}.json"
        tmp_path = File.join(@tmp_dir, "#{payload.id}.json.tmp")
        ready_path = File.join(@ready_dir, filename)

        File.write(tmp_path, payload.to_json)
        File.rename(tmp_path, ready_path)
      end

      def reserve(wait : Time::Span? = nil) : Reservation?
        deadline = wait ? Time.instant + wait : nil

        loop do
          reservation = try_reserve
          return reservation if reservation

          break unless deadline

          remaining = deadline - Time.instant
          return nil if remaining <= Time::Span.zero

          sleep_duration = remaining < @poll_interval ? remaining : @poll_interval
          sleep sleep_duration
        end

        nil
      end

      private def try_reserve : Reservation?
        Dir.children(@ready_dir).sort.each do |filename|
          ready_path = File.join(@ready_dir, filename)
          processing_path = File.join(@processing_dir, filename)

          begin
            File.rename(ready_path, processing_path)
          rescue
            next
          end

          begin
            payload = JobPayload.from_json(File.read(processing_path))
            return reservation_for(payload, processing_path)
          rescue error
            move_to_failed(processing_path, error)
            next
          end
        end

        nil
      end

      private def reservation_for(payload : JobPayload, processing_path : String) : FileReservation
        execution_lock = File.open(lock_path_for(payload.job_class), "a+")
        wait_for_execution_lock(execution_lock)
        begin
          throttle_job_execution(payload.job_class)
          FileReservation.new(payload, processing_path, @failed_dir, execution_lock)
        rescue error
          begin
            execution_lock.flock_unlock
          rescue
          end

          begin
            execution_lock.close
          rescue
          end

          raise error
        end
      end

      private def wait_for_execution_lock(execution_lock : File) : Nil
        # Non-blocking flock attempts keep the scheduler responsive while another
        # worker process/fiber still owns the class execution lock.
        loop do
          begin
            execution_lock.flock_exclusive(false)
            return
          rescue error : IO::Error
            raise error unless error.message.try(&.includes?("already locked"))
            sleep 1.millisecond
          end
        end
      end

      private def throttle_job_execution(job_class : String) : Nil
        config = Crumble::Jobs.throttle_config_for(job_class)
        return unless config

        timespan_milliseconds = config.timespan.total_milliseconds.to_i64
        loop do
          state = read_throttle_state(job_class)
          now = unix_milliseconds
          window_started_at = state.window_started_at_unix_ms

          # Keep a fixed execution window per class that starts with the first job in a burst.
          if window_started_at.nil? || now - window_started_at >= timespan_milliseconds
            state.window_started_at_unix_ms = now
            state.jobs_started_in_window = 0
            window_started_at = now
          end

          if state.jobs_started_in_window < config.max_jobs
            state.jobs_started_in_window += 1
            write_throttle_state(job_class, state)
            return
          end

          wait_for = timespan_milliseconds - (now - window_started_at)
          sleep(wait_for.milliseconds) if wait_for > 0
        end
      end

      private def move_to_failed(path : String, error : Exception) : Nil
        failed_path = File.join(@failed_dir, File.basename(path))

        begin
          File.rename(path, failed_path)
        rescue
          File.delete(path) if File.exists?(path)
          return
        end

        File.write("#{failed_path}.error", error.message)
      end

      private def ensure_dirs : Nil
        Dir.mkdir_p(@ready_dir)
        Dir.mkdir_p(@processing_dir)
        Dir.mkdir_p(@failed_dir)
        Dir.mkdir_p(@tmp_dir)
        Dir.mkdir_p(@throttle_dir)
      end

      private def timestamp_prefix : String
        now = Time.utc
        millis = now.to_unix * 1000 + now.nanosecond / 1_000_000
        millis.to_s
      end

      private def lock_path_for(job_class : String) : String
        File.join(@throttle_dir, "#{job_class_key(job_class)}.lock")
      end

      private def state_path_for(job_class : String) : String
        File.join(@throttle_dir, "#{job_class_key(job_class)}.json")
      end

      private def read_throttle_state(job_class : String) : ThrottleWindowState
        path = state_path_for(job_class)
        return ThrottleWindowState.new unless File.exists?(path)

        ThrottleWindowState.from_json(File.read(path))
      rescue
        ThrottleWindowState.new
      end

      private def write_throttle_state(job_class : String, state : ThrottleWindowState) : Nil
        path = state_path_for(job_class)
        tmp_path = "#{path}.tmp"
        File.write(tmp_path, state.to_json)
        File.rename(tmp_path, path)
      end

      private def job_class_key(job_class : String) : String
        Digest::SHA1.hexdigest(job_class)
      end

      private def unix_milliseconds : Int64
        now = Time.utc
        now.to_unix * 1000 + now.nanosecond // 1_000_000
      end
    end
  end
end
