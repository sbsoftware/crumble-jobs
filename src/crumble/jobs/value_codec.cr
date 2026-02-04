require "json"

module Crumble
  module Jobs
    alias ParamValue = String | Int32 | Int64 | Float32 | Float64 | Time
    alias StoredValue = String | Int64 | Float64 | Time

    struct EncodedValue
      include JSON::Serializable

      getter t : String
      getter v : String | Int64 | Float64

      def initialize(@t : String, @v : String | Int64 | Float64)
      end

      def self.from(value : ParamValue) : self
        case value
        when String
          new("string", value)
        when Int32
          new("int", value.to_i64)
        when Int64
          new("int", value)
        when Float32
          new("float", value.to_f64)
        when Float64
          new("float", value)
        when Time
          new("time", value.to_unix)
        else
          raise ArgumentError.new("Unsupported job parameter type: #{value.class}")
        end
      end

      def to_value : StoredValue
        case @t
        when "string"
          @v.as(String)
        when "int"
          @v.as(Int64)
        when "float"
          @v.as(Float64)
        when "time"
          Time.unix(@v.as(Int64))
        else
          raise ArgumentError.new("Unsupported encoded value type: #{@t}")
        end
      end
    end

    module ValueCaster
      def self.cast(value : StoredValue, type : String.class) : String
        value.as(String)
      end

      def self.cast(value : StoredValue, type : Int32.class) : Int32
        value.as(Int64).to_i32
      end

      def self.cast(value : StoredValue, type : Int64.class) : Int64
        value.as(Int64)
      end

      def self.cast(value : StoredValue, type : Float32.class) : Float32
        value.as(Float64).to_f32
      end

      def self.cast(value : StoredValue, type : Float64.class) : Float64
        value.as(Float64)
      end

      def self.cast(value : StoredValue, type : Time.class) : Time
        value.as(Time)
      end
    end
  end
end
