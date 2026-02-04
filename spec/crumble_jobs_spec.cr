require "./spec_helper"

Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new

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

describe Crumble::Jobs do
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
