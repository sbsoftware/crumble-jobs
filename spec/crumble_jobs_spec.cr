require "./spec_helper"

class ExampleJob < Crumble::Jobs::Job
  params message : String, count : Int32, run_at : Time

  @@runs = [] of String

  def perform : Nil
    @@runs << "#{@message}|#{@count}|#{@run_at.to_unix}"
  end

  def self.runs : Array(String)
    @@runs
  end

  def self.clear : Nil
    @@runs.clear
  end
end

class EmptyJob < Crumble::Jobs::Job
  @@runs = 0

  def perform : Nil
    @@runs += 1
  end

  def self.runs : Int32
    @@runs
  end

  def self.clear : Nil
    @@runs = 0
  end
end

describe Crumble::Jobs do
  before_each do
    Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
  end

  it "enqueues and performs in-memory jobs" do
    ExampleJob.clear

    run_at = Time.utc
    ExampleJob.enqueue(message: "hi", count: 2, run_at: run_at)

    worker = Crumble::Jobs::Worker.new(poll_interval: 10.milliseconds)
    worker.run_once(10.milliseconds).should be_true

    deadline = Time.instant + 100.milliseconds
    until ExampleJob.runs.size == 1 || Time.instant >= deadline
      sleep 1.millisecond
    end

    ExampleJob.runs.should eq([
      "hi|2|#{run_at.to_unix}",
    ])
  end

  it "enqueues and performs parameterless jobs without params" do
    EmptyJob.clear

    EmptyJob.enqueue

    worker = Crumble::Jobs::Worker.new(poll_interval: 10.milliseconds)
    worker.run_once(10.milliseconds).should be_true

    deadline = Time.instant + 100.milliseconds
    until EmptyJob.runs == 1 || Time.instant >= deadline
      sleep 1.millisecond
    end

    EmptyJob.runs.should eq(1)
  end

  it "rejects arguments when deserializing parameterless jobs without params" do
    payload = Crumble::Jobs::JobPayload.new(
      id: "payload-empty",
      job_class: EmptyJob.job_name,
      args: [Crumble::Jobs::EncodedValue.from("unexpected")],
      enqueued_at: Time.utc.to_unix,
    )

    expect_raises(ArgumentError, "Expected 0 args for EmptyJob, got 1") do
      Crumble::Jobs.deserialize(payload)
    end
  end

  it "deserializes job payloads without a registry" do
    ExampleJob.clear

    run_at = Time.utc
    payload = Crumble::Jobs::JobPayload.new(
      id: "payload-1",
      job_class: ExampleJob.job_name,
      args: [
        Crumble::Jobs::EncodedValue.from("hello"),
        Crumble::Jobs::EncodedValue.from(3),
        Crumble::Jobs::EncodedValue.from(run_at),
      ],
      enqueued_at: run_at.to_unix,
    )

    job = Crumble::Jobs.deserialize(payload)
    job.should be_a(ExampleJob)

    example_job = job.as(ExampleJob)
    example_job.message.should eq("hello")
    example_job.count.should eq(3)
    example_job.run_at.to_unix.should eq(run_at.to_unix)
  end

  it "enqueues job instances after a wait" do
    queue = Crumble::Jobs::InMemoryQueue.new
    Crumble::Jobs.configure_queue queue

    ExampleJob.new(message: "later", count: 1, run_at: Time.utc).enqueue(wait: 80.milliseconds)
    queue.reserve(20.milliseconds).should be_nil
    queue.reserve(200.milliseconds).should_not be_nil
  end

  it "enqueues parameterized jobs after an interval" do
    queue = Crumble::Jobs::InMemoryQueue.new
    Crumble::Jobs.configure_queue queue

    ExampleJob.enqueue_in(80.milliseconds, message: "later", count: 1, run_at: Time.utc)
    queue.reserve(20.milliseconds).should be_nil
    queue.reserve(200.milliseconds).should_not be_nil
  end

  it "enqueues parameterless jobs at a time" do
    queue = Crumble::Jobs::InMemoryQueue.new
    Crumble::Jobs.configure_queue queue

    EmptyJob.enqueue_at(Time.utc + 80.milliseconds)
    queue.reserve(20.milliseconds).should be_nil
    queue.reserve(200.milliseconds).should_not be_nil
  end

  it "raises UnknownJobError for unknown job classes" do
    payload = Crumble::Jobs::JobPayload.new(
      id: "payload-2",
      job_class: "MissingJob",
      args: [] of Crumble::Jobs::EncodedValue,
      enqueued_at: Time.utc.to_unix,
    )

    expect_raises(Crumble::Jobs::UnknownJobError) do
      Crumble::Jobs.deserialize(payload)
    end
  end
end
