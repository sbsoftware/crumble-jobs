require "./spec_helper"
require "file_utils"
require "uuid"

class PeriodicPacingJob < Crumble::Jobs::Job
  periodic every: 40.milliseconds
  params

  @@runs = [] of Time::Instant

  def perform : Nil
    @@runs << Time.instant
  end

  def self.runs : Array(Time::Instant)
    @@runs
  end

  def self.clear : Nil
    @@runs.clear
  end
end

class PeriodicFailureJob < Crumble::Jobs::Job
  periodic every: 30.milliseconds
  params

  @@runs = 0

  def perform : Nil
    @@runs += 1
    raise "first run fails" if @@runs == 1
  end

  def self.runs : Int32
    @@runs
  end

  def self.clear : Nil
    @@runs = 0
  end
end

describe "periodic jobs" do
  it "registers a declarative interval" do
    PeriodicPacingJob.periodic_interval.should eq(40.milliseconds)
    Crumble::Jobs.periodic_interval_for(PeriodicPacingJob.job_name).should eq(40.milliseconds)
  end

  it "seeds all registered periodic jobs" do
    queue = Crumble::Jobs::InMemoryQueue.new

    Crumble::Jobs.seed_periodic_jobs(queue).should eq(2)
    Crumble::Jobs.seed_periodic_jobs(queue).should eq(0)
  end

  it "seeds an in-memory periodic job only once" do
    queue = Crumble::Jobs::InMemoryQueue.new

    PeriodicPacingJob.seed_periodic(queue).should be_true
    PeriodicPacingJob.seed_periodic(queue).should be_false
    reservation = queue.reserve(10.milliseconds).not_nil!
    reservation.payload.job_class.should eq(PeriodicPacingJob.job_name)
    reservation.ack
    queue.reserve(10.milliseconds).should be_nil
  end

  it "deduplicates seed-on-restart with a file queue" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-periodic-#{UUID.random}")

    begin
      PeriodicPacingJob.seed_periodic(Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 2.milliseconds)).should be_true
      Dir.children(File.join(queue_root, "ready")).first.should contain("-PeriodicPacingJob.job.")
      restarted_queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 2.milliseconds)
      PeriodicPacingJob.seed_periodic(restarted_queue).should be_false
      reservation = restarted_queue.reserve(20.milliseconds).not_nil!
      reservation.payload.job_class.should eq(PeriodicPacingJob.job_name)
      reservation.ack
      restarted_queue.reserve(10.milliseconds).should be_nil
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "deduplicates legacy file queue entries without class metadata in the filename" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-periodic-legacy-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 2.milliseconds)
      payload = PeriodicPacingJob.new.to_payload
      File.write(File.join(queue_root, "ready", "#{Time.utc.to_unix_ms}-1-#{payload.id}.json"), payload.to_json)

      PeriodicPacingJob.seed_periodic(queue).should be_false
      Dir.children(File.join(queue_root, "ready")).size.should eq(1)
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "does not seed when a legacy queue entry cannot be inspected" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-periodic-invalid-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 2.milliseconds)
      File.write(File.join(queue_root, "ready", "#{Time.utc.to_unix_ms}-1-unknown.json"), "invalid")

      expect_raises(JSON::ParseException) { PeriodicPacingJob.seed_periodic(queue) }
      Dir.children(File.join(queue_root, "ready")).size.should eq(1)
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "paces subsequent occurrences from completion" do
    queue = Crumble::Jobs::InMemoryQueue.new
    PeriodicPacingJob.clear
    PeriodicPacingJob.seed_periodic(queue)
    worker = Crumble::Jobs::Worker.new(queue: queue, poll_interval: 1.millisecond)

    worker.run_once(10.milliseconds).should be_true
    deadline = Time.instant + 1.second
    until PeriodicPacingJob.runs.size == 1 || Time.instant >= deadline
      sleep 1.millisecond
    end
    worker.run_once(10.milliseconds).should be_false
    worker.run_once(100.milliseconds).should be_true
    until PeriodicPacingJob.runs.size == 2 || Time.instant >= deadline
      sleep 1.millisecond
    end

    PeriodicPacingJob.runs.size.should eq(2)
    (PeriodicPacingJob.runs[1] - PeriodicPacingJob.runs[0]).should be >= 35.milliseconds
  end

  it "keeps the periodic chain alive after a failed occurrence" do
    queue = Crumble::Jobs::InMemoryQueue.new
    PeriodicFailureJob.clear
    PeriodicFailureJob.seed_periodic(queue)
    worker = Crumble::Jobs::Worker.new(queue: queue, poll_interval: 1.millisecond)

    worker.run_once(10.milliseconds).should be_true
    deadline = Time.instant + 1.second
    until PeriodicFailureJob.runs == 1 || Time.instant >= deadline
      sleep 1.millisecond
    end
    worker.run_once(100.milliseconds).should be_true
    until PeriodicFailureJob.runs == 2 || Time.instant >= deadline
      sleep 1.millisecond
    end

    PeriodicFailureJob.runs.should eq(2)
  end
end
