require "./spec_helper"
require "file_utils"
require "uuid"

class ShutdownBlockingJob < Crumble::Jobs::Job
  @@started = Channel(Nil).new(1)
  @@finish = Channel(Nil).new(1)

  def perform : Nil
    @@started.send(nil)
    @@finish.receive
  end

  def self.wait_until_started : Nil
    @@started.receive
  end

  def self.finish : Nil
    @@finish.send(nil)
  end
end

describe Crumble::Jobs::FileQueue do
  it "recovers an unlocked processing file after restart" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-recovery-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      payload = Crumble::Jobs::JobPayload.new(id: "abandoned", job_class: "AbandonedJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
      queue.enqueue(payload)
      filename = Dir.children(File.join(queue_root, "ready")).first
      File.rename(File.join(queue_root, "ready", filename), File.join(queue_root, "processing", filename))

      restarted_queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      reservation = restarted_queue.reserve(20.milliseconds)
      reservation.should_not be_nil
      reservation.not_nil!.payload.id.should eq("abandoned")
      reservation.not_nil!.ack
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "does not recover a processing file locked by a live worker" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-live-reservation-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      payload = Crumble::Jobs::JobPayload.new(id: "owned", job_class: "OwnedJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
      queue.enqueue(payload)
      reservation = queue.reserve(20.milliseconds).not_nil!

      competing_queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      competing_queue.reserve(10.milliseconds).should be_nil
      Dir.children(File.join(queue_root, "processing")).size.should eq(1)
      reservation.ack
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "makes an abandoned job executable instead of permanently blocking its class" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-pending-recovery-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      payload = Crumble::Jobs::JobPayload.new(id: "pending-abandoned", job_class: "PendingJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
      queue.enqueue(payload)
      filename = Dir.children(File.join(queue_root, "ready")).first
      File.rename(File.join(queue_root, "ready", filename), File.join(queue_root, "processing", filename))

      replacement = Crumble::Jobs::JobPayload.new(id: "replacement", job_class: "PendingJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
      queue.enqueue_unless_pending(replacement).should be_false
      reservation = queue.reserve(20.milliseconds).not_nil!
      reservation.payload.id.should eq("pending-abandoned")
      reservation.ack
      queue.enqueue_unless_pending(replacement).should be_true
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end

  it "releases an active reservation when graceful shutdown times out" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-shutdown-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 1.millisecond)
      queue.enqueue(ShutdownBlockingJob.new.to_payload)
      worker = Crumble::Jobs::Worker.new(queue: queue, poll_interval: 1.millisecond)
      worker.run_once(20.milliseconds).should be_true
      ShutdownBlockingJob.wait_until_started

      worker.shutdown(Time::Span.zero)
      Dir.children(File.join(queue_root, "processing")).should be_empty
      Dir.children(File.join(queue_root, "ready")).size.should eq(1)
      ShutdownBlockingJob.finish
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end
end
