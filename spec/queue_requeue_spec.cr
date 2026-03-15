require "./spec_helper"
require "file_utils"
require "uuid"

describe "queue-level delayed requeue" do
  it "requeues in-memory payloads for a future time" do
    queue = Crumble::Jobs::InMemoryQueue.new
    payload = Crumble::Jobs::JobPayload.new(id: "delayed-memory", job_class: "DelayedMemoryJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
    queue.requeue_at(payload, Time.utc + 80.milliseconds)
    queue.reserve(20.milliseconds).should be_nil

    reservation = queue.reserve(200.milliseconds)
    reservation.should_not be_nil
    reservation.not_nil!.payload.id.should eq("delayed-memory")
    reservation.not_nil!.ack
  end

  it "requeues file payloads for a future time" do
    queue_root = File.join(Dir.tempdir, "crumble-jobs-requeue-#{UUID.random}")

    begin
      queue = Crumble::Jobs::FileQueue.new(queue_root, poll_interval: 5.milliseconds)
      payload = Crumble::Jobs::JobPayload.new(id: "delayed-file", job_class: "DelayedFileJob", args: [] of Crumble::Jobs::EncodedValue, enqueued_at: Time.utc.to_unix)
      queue.requeue_at(payload, Time.utc + 80.milliseconds)
      queue.reserve(20.milliseconds).should be_nil

      reservation = queue.reserve(300.milliseconds)
      reservation.should_not be_nil
      reservation.not_nil!.payload.id.should eq("delayed-file")
      reservation.not_nil!.ack
    ensure
      FileUtils.rm_r(queue_root) if Dir.exists?(queue_root)
    end
  end
end
