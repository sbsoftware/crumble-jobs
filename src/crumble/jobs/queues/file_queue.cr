require "../queue_backend"
require "../throttle_state"
require "digest/sha1"

module Crumble
  module Jobs
    class FileReservation < Reservation
      def initialize(payload : JobPayload, @path : String, @failed_dir : String)
        super(payload)
      end

      def ack : Nil
        File.delete(@path) if File.exists?(@path)
      rescue
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
        @reserve_lock_path = File.join(@root, "reserve.lock")
        @filename_sequence = 0_i64
        @filename_sequence_lock = Mutex.new
        ensure_dirs
      end

      def enqueue(payload : JobPayload, run_at : Time = Time.utc) : Nil
        filename = "#{run_at.to_unix_ms}-#{next_filename_sequence}-#{payload.id}.json"
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
        reserve_lock = File.open(@reserve_lock_path, "a+")
        wait_for_file_lock(reserve_lock)
        now_unix_ms = unix_milliseconds

        Dir.children(@ready_dir).sort.each do |filename|
          ready_at_unix_ms = ready_at_unix_ms(filename)
          break if ready_at_unix_ms && ready_at_unix_ms > now_unix_ms

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
      ensure
        release_file_lock(reserve_lock) if reserve_lock
      end

      private def reservation_for(payload : JobPayload, processing_path : String) : FileReservation?
        if run_at_unix_ms = throttle_requeue_at(payload.job_class)
          requeue_processing_file_at(payload, processing_path, run_at_unix_ms)
          return nil
        end

        FileReservation.new(payload, processing_path, @failed_dir)
      end

      private def throttle_requeue_at(job_class : String) : Int64?
        config = Crumble::Jobs.throttle_config_for(job_class)
        return nil unless config

        throttle_lock = File.open(lock_path_for(job_class), "a+")
        wait_for_file_lock(throttle_lock)

        timespan_milliseconds = config.timespan.total_milliseconds.to_i64
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
          return nil
        end

        window_started_at + timespan_milliseconds
      ensure
        release_file_lock(throttle_lock) if throttle_lock
      end

      private def wait_for_file_lock(lock_file : File) : Nil
        loop do
          return if try_lock_file(lock_file)
          sleep 1.millisecond
        end
      end

      private def try_lock_file(lock_file : File) : Bool
        lock_file.flock_exclusive(false)
        true
      rescue error : IO::Error
        raise error unless error.message.try(&.includes?("already locked"))
        false
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

      private def ready_at_unix_ms(filename : String) : Int64?
        filename.split("-", 2)[0]?.try(&.to_i64?)
      end

      private def requeue_processing_file_at(payload : JobPayload, processing_path : String, run_at_unix_ms : Int64) : Nil
        ready_path = File.join(@ready_dir, "#{run_at_unix_ms}-#{File.basename(processing_path)}")
        begin
          File.rename(processing_path, ready_path)
        rescue
          enqueue(payload, run_at: Time.unix_ms(run_at_unix_ms))
          File.delete(processing_path) if File.exists?(processing_path)
        end
      end

      private def release_file_lock(lock_file : File) : Nil
        begin
          lock_file.flock_unlock
        rescue
        end

        begin
          lock_file.close
        rescue
        end
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

      private def next_filename_sequence : Int64
        @filename_sequence_lock.synchronize do
          @filename_sequence += 1
        end
      end
    end
  end
end
