
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Delayer do
  it "should copy expired messages to their target streams" do
    redis.xadd Redstream.stream_key_name("target.delay"), "*", "payload", JSON.dump(value: "message")

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(0)

    Redstream::Delayer.new(stream_name: "target", delay: 0).run_once

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
    expect(redis.xrange(Redstream.stream_key_name("target"), "-", "+").last[1]).to eq(["payload", JSON.dump(value: "message")])
  end

  it "should deliver and commit before falling asleep" do
    redis.xadd Redstream.stream_key_name("target.delay"), "*", "payload", JSON.dump(value: "message")
    sleep 3
    redis.xadd Redstream.stream_key_name("target.delay"), "*", "payload", JSON.dump(value: "message")

    thread = Thread.new do
      Redstream::Delayer.new(stream_name: "target", delay: 1).run_once
    end

    sleep 1

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
    expect(redis.get(Redstream.offset_key_name(stream_name: "target.delay", consumer_name: "delayer"))).not_to be_nil

    thread.join

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(2)
  end

  it "shouldn't copy not yet expired messages" do
    redis.xadd Redstream.stream_key_name("target.delay"), "*", "payload", JSON.dump(value: "message")

    thread = Thread.new do
      Redstream::Delayer.new(stream_name: "target", delay: 2).run_once
    end

    sleep 1

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(0)

    thread.join

    expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
  end
end

