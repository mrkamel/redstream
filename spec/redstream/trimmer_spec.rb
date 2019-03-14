
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Trimmer do
  it "should trim a stream to the minimum committed id" do
    ids = Array.new(4) do |i|
      redis.xadd Redstream.stream_key_name("default"), payload: JSON.dump(value: "message#{i}")
    end

    redis.set Redstream.offset_key_name(stream_name: "default", consumer_name: "consumer1"), ids[1]
    redis.set Redstream.offset_key_name(stream_name: "default", consumer_name: "consumer2"), ids[2]

    trimmer = Redstream::Trimmer.new(
      interval: 5,
      stream_name: "default",
      consumer_names: ["consumer1", "consumer2", "consumer_without_committed_id"]
    )

    trimmer.run_once

    expect(redis.xlen(Redstream.stream_key_name("default"))).to eq(2)
  end

  it "should sleep for the specified time if there's nothing to trim" do
    trimmer = Redstream::Trimmer.new(interval: 1, stream_name: "default", consumer_names: ["unknown_consumer"])
    trimmer.expects(:sleep).with(1).returns(true)
    trimmer.run_once
  end
end

