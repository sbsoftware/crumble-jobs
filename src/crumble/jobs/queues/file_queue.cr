require "../queue_backend"

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
            return FileReservation.new(payload, processing_path, @failed_dir)
          rescue error
            move_to_failed(processing_path, error)
            next
          end
        end

        nil
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
