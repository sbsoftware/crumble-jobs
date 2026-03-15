require "./spec_helper"
require "file_utils"
require "uuid"

class ThrottledJob < Crumble::Jobs::Job
  throttle max_jobs: 2, timespan: 80.milliseconds
  params sequence : Int32

  @@starts = Channel(Tuple(Int32, Time::Instant)).new(100)

  def perform : Nil
    @@starts.send({@sequence, Time.instant})
  end

  def self.poll_start : Tuple(Int32, Time::Instant)?
    select
    when start = @@starts.receive
      start
    when timeout(0.milliseconds)
      nil
    end
  end

  def self.clear : Nil
    loop do
      start = poll_start
      break unless start
    end
  end
end

describe "job throttling" do
  it "throttles in-memory jobs at execution time and keeps order within the class" do
    Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
    ThrottledJob.clear

    3.times { |index| ThrottledJob.enqueue(sequence: index + 1) }

    starts = [] of Tuple(Int32, Time::Instant)
    worker = Crumble::Jobs::Worker.new(max_concurrency: 3, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until starts.size == 3 || Time.instant >= deadline
      worker.run_once(10.milliseconds)
      while start = ThrottledJob.poll_start
        starts << start
      end
      sleep 2.milliseconds
    end

    starts.size.should eq(3)
    starts.map(&.[0]).should eq([1, 2, 3])
    (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
    (starts[1][1] - starts[0][1]).should be < 70.milliseconds
  end

  it "shares throttle state across workers that reserve from the same queue" do
    queue = Crumble::Jobs::InMemoryQueue.new
    Crumble::Jobs.configure_queue queue
    ThrottledJob.clear

    3.times { |index| ThrottledJob.enqueue(sequence: index + 1) }

    starts = [] of Tuple(Int32, Time::Instant)
    worker_one = Crumble::Jobs::Worker.new(queue: queue, max_concurrency: 1, poll_interval: 1.millisecond)
    worker_two = Crumble::Jobs::Worker.new(queue: queue, max_concurrency: 1, poll_interval: 1.millisecond)
    deadline = Time.instant + 2.seconds
    until starts.size == 3 || Time.instant >= deadline
      worker_one.run_once(10.milliseconds)
      worker_two.run_once(10.milliseconds)
      while start = ThrottledJob.poll_start
        starts << start
      end
      sleep 2.milliseconds
    end

    starts.size.should eq(3)
    starts.map(&.[0]).should eq([1, 2, 3])
    (starts[2][1] - starts[0][1]).should be >= 70.milliseconds
    (starts[1][1] - starts[0][1]).should be < 70.milliseconds
  end

  it "throttles file-queued jobs at execution time and keeps order within the class" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-throttle-#{UUID.random}")

    begin
      Crumble::Jobs.configure_queue Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      ThrottledJob.clear

      3.times { |index| ThrottledJob.enqueue(sequence: index + 1) }

      starts = [] of Tuple(Int32, Time::Instant)
      worker = Crumble::Jobs::Worker.new(max_concurrency: 3, poll_interval: 1.millisecond)
      deadline = Time.instant + 2.seconds
      until starts.size == 3 || Time.instant >= deadline
        worker.run_once(10.milliseconds)
        while start = ThrottledJob.poll_start
          starts << start
        end
        sleep 2.milliseconds
      end

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
      ThrottledJob.clear

      3.times { |index| ThrottledJob.enqueue(sequence: index + 1) }

      starts = [] of Tuple(Int32, Time::Instant)
      worker_one = Crumble::Jobs::Worker.new(queue: queue_one, max_concurrency: 1, poll_interval: 1.millisecond)
      worker_two = Crumble::Jobs::Worker.new(queue: queue_two, max_concurrency: 1, poll_interval: 1.millisecond)
      deadline = Time.instant + 2.seconds
      until starts.size == 3 || Time.instant >= deadline
        worker_one.run_once(10.milliseconds)
        worker_two.run_once(10.milliseconds)
        while start = ThrottledJob.poll_start
          starts << start
        end
        sleep 2.milliseconds
      end

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
