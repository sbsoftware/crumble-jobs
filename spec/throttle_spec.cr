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
    3.times { worker.run_once(50.milliseconds).should be_true }

    deadline = Time.instant + 2.seconds
    until ThrottledInMemoryJob.starts.size == 3 || Time.instant >= deadline
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
      3.times { worker.run_once(500.milliseconds).should be_true }

      deadline = Time.instant + 2.seconds
      until ThrottledFileJob.starts.size == 3 || Time.instant >= deadline
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
end
