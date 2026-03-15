require "./spec_helper"
require "file_utils"
require "uuid"

class ThrottledInMemoryJob < Crumble::Jobs::Job
  throttle max_jobs: 2, timespan: 80.milliseconds
  params sequence : Int32

  @@starts = [] of Tuple(Int32, Time::Instant)
  @@lock = Mutex.new

  def perform : Nil
    @@lock.synchronize { @@starts << {@sequence, Time.instant} }
  end

  def self.starts : Array(Tuple(Int32, Time::Instant))
    @@lock.synchronize { @@starts.dup }
  end

  def self.clear : Nil
    @@lock.synchronize { @@starts.clear }
  end
end

class ThrottledFileJob < Crumble::Jobs::Job
  throttle max_jobs: 2, timespan: 80.milliseconds
  params sequence : Int32

  @@starts = [] of Tuple(Int32, Time::Instant)
  @@lock = Mutex.new

  def perform : Nil
    @@lock.synchronize { @@starts << {@sequence, Time.instant} }
  end

  def self.starts : Array(Tuple(Int32, Time::Instant))
    @@lock.synchronize { @@starts.dup }
  end

  def self.clear : Nil
    @@lock.synchronize { @@starts.clear }
  end
end

describe "job throttling" do
  it "throttles in-memory jobs at execution time and keeps order within the class" do
    Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    ThrottledInMemoryJob.clear

    3.times { |index| ThrottledInMemoryJob.enqueue(sequence: index + 1) }

    worker = Crumble::Jobs::Worker.new(max_concurrency: 3, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until ThrottledInMemoryJob.starts.size == 3 || Time.instant >= deadline
      worker.run_once(10.milliseconds)
      sleep 2.milliseconds
    end

    starts = ThrottledInMemoryJob.starts
    starts.size.should eq(3)
    starts.map(&.[0]).should eq([1, 2, 3])
    (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
    (starts[1][1] - starts[0][1]).should be < 70.milliseconds
  end

  it "shares throttle state across workers that reserve from the same queue" do
    queue = Crumble::Jobs::InMemoryQueue.new
    Crumble::Jobs.configure_queue queue
    ThrottledInMemoryJob.clear

    3.times { |index| ThrottledInMemoryJob.enqueue(sequence: index + 1) }

    worker_one = Crumble::Jobs::Worker.new(queue: queue, max_concurrency: 1, poll_interval: 1.millisecond)
    worker_two = Crumble::Jobs::Worker.new(queue: queue, max_concurrency: 1, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until ThrottledInMemoryJob.starts.size == 3 || Time.instant >= deadline
      worker_one.run_once(10.milliseconds)
      worker_two.run_once(10.milliseconds)
      sleep 2.milliseconds
    end

    starts = ThrottledInMemoryJob.starts
    starts.size.should eq(3)
    starts.map(&.[0]).should eq([1, 2, 3])
    (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
    (starts[1][1] - starts[0][1]).should be < 70.milliseconds
  end

  it "throttles file-queued jobs at execution time and keeps order within the class" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-throttle-#{UUID.random}")

    begin
      Crumble::Jobs.configure_queue Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      ThrottledFileJob.clear

      3.times { |index| ThrottledFileJob.enqueue(sequence: index + 1) }

      worker = Crumble::Jobs::Worker.new(max_concurrency: 3, poll_interval: 1.millisecond)
      deadline = Time.instant + 2.seconds
      until ThrottledFileJob.starts.size == 3 || Time.instant >= deadline
        worker.run_once(10.milliseconds)
        sleep 2.milliseconds
      end

      starts = ThrottledFileJob.starts
      starts.size.should eq(3)
      starts.map(&.[0]).should eq([1, 2, 3])
      (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
      Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    end
  end

  it "shares throttle state across file queue instances through persisted files" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-throttle-shared-#{UUID.random}")

    begin
      queue_one = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      queue_two = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      Crumble::Jobs.configure_queue queue_one
      ThrottledFileJob.clear

      3.times { |index| ThrottledFileJob.enqueue(sequence: index + 1) }

      worker_one = Crumble::Jobs::Worker.new(queue: queue_one, max_concurrency: 1, poll_interval: 1.millisecond)
      worker_two = Crumble::Jobs::Worker.new(queue: queue_two, max_concurrency: 1, poll_interval: 1.millisecond)
      deadline = Time.instant + 2.seconds
      until ThrottledFileJob.starts.size == 3 || Time.instant >= deadline
        worker_one.run_once(10.milliseconds)
        worker_two.run_once(10.milliseconds)
        sleep 2.milliseconds
      end

      starts = ThrottledFileJob.starts
      starts.size.should eq(3)
      starts.map(&.[0]).should eq([1, 2, 3])
      (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
      (starts[1][1] - starts[0][1]).should be < 70.milliseconds
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
      Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    end
  end
end
