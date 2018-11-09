
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Delayer do
  it "should copy expired messages to their target streams" do
    targets = ["target1", "target2"]

    targets.each do |target|
      redis.xadd Redstream.stream_key_name("delay"), "*", "payload", JSON.dump(value: "message"), "stream_name", target
    end

    targets.each do |target|
      expect(redis.xlen(Redstream.stream_key_name(target))).to eq(0)
    end

    Redstream::Delayer.new(delay: 0, value: "localhost").run_once

    targets.each do |target|
      expect(redis.xlen(Redstream.stream_key_name(target))).to eq(1)
      expect(array_to_hash(redis.xrange(Redstream.stream_key_name(target), "-", "+").last[1])["payload"]).to eq(JSON.dump(value: "message"))
    end
  end

  it "shouldn't copy not yet expired messages" do
    redis.xadd Redstream.stream_key_name("delay"), "*", "payload", JSON.dump(value: "message"), "stream_name", "target"

    thread = Thread.new do
      Redstream::Delayer.new(delay: 2, value: "localhost").run_once
    end

    sleep 1

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(0)

    thread.join

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
  end
end

