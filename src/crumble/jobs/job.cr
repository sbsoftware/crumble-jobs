require "uuid"

require "./job_payload"
require "./queue_backend"
require "./value_codec"

module Crumble
  module Jobs
    abstract class Job
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
