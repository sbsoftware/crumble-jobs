require "./jobs/value_codec"
require "./jobs/job_payload"
require "./jobs/queue_backend"
require "./jobs/job"
require "./jobs/worker"
require "./jobs/queues/in_memory_queue"
require "./jobs/queues/file_queue"

module Crumble
  module Jobs
    macro configure_queue(queue)
      Crumble::Jobs.set_queue({{queue}})
    end

    @@queue : QueueBackend? = nil

    def self.set_queue(queue : QueueBackend) : Nil
      @@queue = queue
    end

    def self.queue : QueueBackend
      @@queue || raise "Crumble::Jobs queue not configured. Call Crumble::Jobs.configure_queue"
    end

    class UnknownJobError < ArgumentError
      getter job_class : String

      def initialize(@job_class : String)
        super("Unknown job class: #{@job_class}")
      end
    end

    def self.deserialize(payload : JobPayload) : Job
      {% begin %}
        case payload.job_class
        {% for job_class in Crumble::Jobs::Job.all_subclasses %}
          {% unless job_class.abstract? %}
          when {{job_class}}.job_name
            {{job_class}}.from_payload(payload)
          {% end %}
        {% end %}
        else
          raise UnknownJobError.new(payload.job_class)
        end
      {% end %}
    end

    def self.throttle_config_for(job_class : String) : ThrottleConfig?
      {% begin %}
        case job_class
        {% for job_class in Crumble::Jobs::Job.all_subclasses %}
          {% unless job_class.abstract? %}
          when {{job_class}}.job_name
            {{job_class}}.throttle_config
          {% end %}
        {% end %}
        else
          nil
        end
      {% end %}
    end

    def self.retry_outcome_for(payload : JobPayload, error : Exception) : RetryOutcome
      {% begin %}
        case payload.job_class
        {% for job_class in Crumble::Jobs::Job.all_subclasses %}
          {% unless job_class.abstract? %}
          when {{job_class}}.job_name
            {{job_class}}.retry_outcome_for(payload, error)
          {% end %}
        {% end %}
        else
          RetryOutcome.new(matched: false, schedule: nil)
        end
      {% end %}
    end
  end
end
