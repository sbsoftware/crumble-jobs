require "../queue_backend"

module Crumble
  module Jobs
    class FileJobClassState
      getter execution_lock : Channel(Nil)
      property window_started_at : Time::Instant?
      property jobs_started_in_window : Int32

      def initialize
        @execution_lock = Channel(Nil).new(1)
        @execution_lock.send(nil)
        @window_started_at = nil
        @jobs_started_in_window = 0
      end
    end

    class FileReservation < Reservation
      def initialize(payload : JobPayload, @path : String, @failed_dir : String, @execution_lock : Channel(Nil))
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
        @execution_lock.send(nil)
      end
    end

    class FileQueue < QueueBackend
      DEFAULT_POLL_INTERVAL = 1.second

      def initialize(@root : String, @poll_interval : Time::Span = DEFAULT_POLL_INTERVAL)
        @ready_dir = File.join(@root, "ready")
        @processing_dir = File.join(@root, "processing")
        @failed_dir = File.join(@root, "failed")
        @tmp_dir = File.join(@root, "tmp")
        @job_class_states = {} of String => FileJobClassState
        @job_class_states_lock = Mutex.new
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
        job_class_state = job_class_state_for(payload.job_class)
        job_class_state.execution_lock.receive
        begin
          throttle_job_execution(payload.job_class, job_class_state)
          FileReservation.new(payload, processing_path, @failed_dir, job_class_state.execution_lock)
        rescue error
          job_class_state.execution_lock.send(nil)
          raise error
        end
      end

      private def job_class_state_for(job_class : String) : FileJobClassState
        @job_class_states_lock.synchronize do
          @job_class_states[job_class]? || begin
            state = FileJobClassState.new
            @job_class_states[job_class] = state
            state
          end
        end
      end

      private def throttle_job_execution(job_class : String, job_class_state : FileJobClassState) : Nil
        config = Crumble::Jobs.throttle_config_for(job_class)
        return unless config

        loop do
          now = Time.instant
          window_started_at = job_class_state.window_started_at

          # Keep a fixed execution window per class that starts with the first job in a burst.
          if window_started_at.nil? || now - window_started_at >= config.timespan
            job_class_state.window_started_at = now
            job_class_state.jobs_started_in_window = 0
            window_started_at = now
          end

          if job_class_state.jobs_started_in_window < config.max_jobs
            job_class_state.jobs_started_in_window += 1
            return
          end

          wait_for = config.timespan - (now - window_started_at)
          sleep(wait_for) if wait_for > Time::Span.zero
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
      end

      private def timestamp_prefix : String
        now = Time.utc
        millis = now.to_unix * 1000 + now.nanosecond / 1_000_000
        millis.to_s
      end
    end
  end
end
