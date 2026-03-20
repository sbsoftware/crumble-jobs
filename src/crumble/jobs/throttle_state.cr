require "json"

module Crumble
  module Jobs
    class ThrottleWindowState
      include JSON::Serializable

      property window_started_at_unix_ms : Int64?
      property jobs_started_in_window : Int32

      def initialize(@window_started_at_unix_ms : Int64? = nil, @jobs_started_in_window : Int32 = 0)
      end
    end
  end
end
