require "./spec_helper"
require "file_utils"
require "uuid"

class RetryableInMemoryError < Exception
end

class RetryableFirstError < Exception
end

class RetryableSecondError < Exception
end

class RetryableFileError < Exception
end

class RetryInMemoryJob < Crumble::Jobs::Job
  retry_on RetryableInMemoryError, attempts: 2, wait: ->(attempt : Int32) { RetryInMemoryJob.wait_for(attempt) }
  params token : String

  @@runs = [] of String
  @@wait_attempts = [] of Int32

  def self.wait_for(attempt : Int32) : Time::Span
    @@wait_attempts << attempt
    20.milliseconds
  end

  def perform : Nil
    @@runs << @token
    raise RetryableInMemoryError.new("in-memory retry #{@token}") if @@runs.size <= 2
  end

  def self.runs : Array(String)
    @@runs
  end

  def self.wait_attempts : Array(Int32)
    @@wait_attempts
  end

  def self.clear : Nil
    @@runs.clear
    @@wait_attempts.clear
  end
end

class RetryIndependentJob < Crumble::Jobs::Job
  retry_on RetryableFirstError, attempts: 1, wait: ->(attempt : Int32) { RetryIndependentJob.wait_for_first(attempt) }
  retry_on RetryableSecondError, attempts: 2, wait: ->(attempt : Int32) { RetryIndependentJob.wait_for_second(attempt) }
  params token : String

  @@runs = Hash(String, Int32).new(0)
  @@successes = [] of String
  @@first_wait_attempts = [] of Int32
  @@second_wait_attempts = [] of Int32

  def self.wait_for_first(attempt : Int32) : Time::Span
    @@first_wait_attempts << attempt
    10.milliseconds
  end

  def self.wait_for_second(attempt : Int32) : Time::Span
    @@second_wait_attempts << attempt
    10.milliseconds
  end

  def perform : Nil
    @@runs[@token] += 1
    case @@runs[@token]
    when 1
      raise RetryableFirstError.new("first #{@token}")
    when 2, 3
      raise RetryableSecondError.new("second #{@token}")
    else
      @@successes << @token
    end
  end

  def self.successes : Array(String)
    @@successes
  end

  def self.first_wait_attempts : Array(Int32)
    @@first_wait_attempts
  end

  def self.second_wait_attempts : Array(Int32)
    @@second_wait_attempts
  end

  def self.clear : Nil
    @@runs.clear
    @@successes.clear
    @@first_wait_attempts.clear
    @@second_wait_attempts.clear
  end
end

class RetryExhaustedFileJob < Crumble::Jobs::Job
  retry_on RetryableFileError, attempts: 1, wait: ->(attempt : Int32) { attempt.milliseconds }
  params token : String

  @@runs = Hash(String, Int32).new(0)

  def perform : Nil
    @@runs[@token] += 1
    raise RetryableFileError.new("file retry #{@token} #{@@runs[@token]}")
  end

  def self.runs_for(token : String) : Int32
    @@runs[token]? || 0
  end

  def self.clear : Nil
    @@runs.clear
  end
end

describe "retry_on" do
  it "retries a configured exception with the wait proc attempt number" do
    Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    RetryInMemoryJob.clear

    RetryInMemoryJob.enqueue(token: "alpha")

    worker = Crumble::Jobs::Worker.new(max_concurrency: 1, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until RetryInMemoryJob.runs.size == 3 || Time.instant >= deadline
      worker.run_once(20.milliseconds)
      sleep 1.millisecond
    end

    RetryInMemoryJob.runs.should eq(["alpha", "alpha", "alpha"])
    RetryInMemoryJob.wait_attempts.should eq([1, 2])
    worker.run_once(20.milliseconds).should be_false
  end

  it "tracks retry counts independently per configured exception class" do
    Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    RetryIndependentJob.clear

    RetryIndependentJob.enqueue(token: "independent")

    worker = Crumble::Jobs::Worker.new(max_concurrency: 1, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until RetryIndependentJob.successes.size == 1 || Time.instant >= deadline
      worker.run_once(20.milliseconds)
      sleep 1.millisecond
    end

    RetryIndependentJob.successes.should eq(["independent"])
    RetryIndependentJob.first_wait_attempts.should eq([1])
    RetryIndependentJob.second_wait_attempts.should eq([1, 2])
    worker.run_once(20.milliseconds).should be_false
  end

  it "persists retry metadata in file queues and fails once retries are exhausted" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-retry-on-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      Crumble::Jobs.configure_queue queue
      RetryExhaustedFileJob.clear

      RetryExhaustedFileJob.enqueue(token: "file")

      worker = Crumble::Jobs::Worker.new(queue: queue, max_concurrency: 1, poll_interval: 1.millisecond)
      deadline = Time.instant + 2.seconds
      until RetryExhaustedFileJob.runs_for("file") == 2 || Time.instant >= deadline
        worker.run_once(20.milliseconds)
        sleep 1.millisecond
      end

      RetryExhaustedFileJob.runs_for("file").should eq(2)
      worker.run_once(20.milliseconds).should be_false

      failed_dir = File.join(queue_root, "failed")
      failed_json_files = Dir.children(failed_dir).select { |name| name.ends_with?(".json") }
      failed_json_files.size.should eq(1)
      File.read(File.join(failed_dir, "#{failed_json_files[0]}.error")).should eq("file retry file 2")
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
      Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    end
  end
end
