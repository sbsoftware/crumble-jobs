require "uuid"

require "./job_payload"
require "./queue_backend"
require "./value_codec"

module Crumble
  module Jobs
    record ThrottleConfig, max_jobs : Int32, timespan : Time::Span
    record RetrySchedule, payload : JobPayload, wait : Time::Span
    record RetryOutcome, matched : Bool, schedule : RetrySchedule?

    abstract class Job
      macro throttle(*, max_jobs, timespan)
        {% if max_jobs.is_a?(NumberLiteral) && max_jobs <= 0 %}
          {{ raise "throttle max_jobs must be greater than 0" }}
        {% end %}

        def self.throttle_config : Crumble::Jobs::ThrottleConfig?
          Crumble::Jobs::ThrottleConfig.new(max_jobs: {{max_jobs}}.to_i32, timespan: {{timespan}})
        end
      end

      macro retry_on(error_class, *, attempts, wait)
        {% if attempts.is_a?(NumberLiteral) && attempts <= 0 %}
          {{ raise "retry_on attempts must be greater than 0" }}
        {% end %}
        {% module_name = "RetryOn_#{error_class.id}".gsub(/::/, "__").id %}

        module {{module_name}}
          def retry_outcome_for(payload : Crumble::Jobs::JobPayload, error : Exception) : Crumble::Jobs::RetryOutcome
            if error.is_a?({{error_class}})
              retry_count = payload.retry_count_for({{error_class.stringify}}) + 1
              return Crumble::Jobs::RetryOutcome.new(matched: true, schedule: nil) if retry_count > {{attempts}}.to_i32

              wait_for = {{wait}}.call(retry_count)
              retry_payload = payload.with_retry_count({{error_class.stringify}}, retry_count)
              return Crumble::Jobs::RetryOutcome.new(matched: true, schedule: Crumble::Jobs::RetrySchedule.new(payload: retry_payload, wait: wait_for))
            end

            super
          end
        end

        extend {{module_name}}
      end

      macro params(*fields)
        {% allowed = ["String", "Int32", "Int64", "Float32", "Float64", "Time"] %}
        {% for field in fields %}
          {% unless allowed.includes?(field.type.stringify) %}
            {{ raise "Unsupported job param type: #{field.type}" }}
          {% end %}
        {% end %}

        {% for field in fields %}
          getter {{field.var}} : {{field.type}}
        {% end %}

        {% if fields.size > 0 %}
          def initialize(
            {% for field, index in fields %}
              @{{field.var}} : {{field.type}}{% if index < fields.size - 1 %}, {% end %}
            {% end %}
          )
          end
        {% else %}
          def initialize
          end
        {% end %}

        def serialize_args : Array(Crumble::Jobs::ParamValue)
          {% if fields.size == 0 %}
            [] of Crumble::Jobs::ParamValue
          {% else %}
            [
              {% for field in fields %}
                @{{field.var}},
              {% end %}
            ] of Crumble::Jobs::ParamValue
          {% end %}
        end

        {% if fields.size > 0 %}
          def self.enqueue(
            {% for field, index in fields %}
              {{field.var}} : {{field.type}}{% if index < fields.size - 1 %}, {% end %}
            {% end %}
          ) : String
            new(
              {% for field in fields %}
                {{field.var}}: {{field.var}},
              {% end %}
            ).enqueue
          end
        {% else %}
          def self.enqueue : String
            new.enqueue
          end
        {% end %}

        def self.from_payload(payload : Crumble::Jobs::JobPayload) : self
          if payload.args.size != {{fields.size}}
            raise ArgumentError.new("Expected {{fields.size}} args for #{self.name}, got #{payload.args.size}")
          end

          {% if fields.size > 0 %}
            new(
              {% for field, index in fields %}
                {{field.var}}: Crumble::Jobs::ValueCaster.cast(payload.args[{{index}}].to_value, {{field.type}}),
              {% end %}
            )
          {% else %}
            new
          {% end %}
        end
      end

      def self.job_name : String
        name.to_s
      end

      def self.throttle_config : Crumble::Jobs::ThrottleConfig?
        nil
      end

      def self.retry_outcome_for(payload : Crumble::Jobs::JobPayload, error : Exception) : Crumble::Jobs::RetryOutcome
        Crumble::Jobs::RetryOutcome.new(matched: false, schedule: nil)
      end

      abstract def perform : Nil
      abstract def serialize_args : Array(Crumble::Jobs::ParamValue)

      def enqueue : String
        now = Time.utc
        encoded_args = serialize_args.map { |value| EncodedValue.from(value) }

        payload = JobPayload.new(
          id: UUID.random.to_s,
          job_class: self.class.job_name,
          args: encoded_args,
          enqueued_at: now.to_unix,
        )

        Crumble::Jobs.queue.enqueue(payload)
        payload.id
      end
    end
  end
end
