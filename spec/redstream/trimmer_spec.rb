
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Trimmer do
  it "should delete expired messages" do
    redis.xadd Redstream.stream_key_name("default"), "*", "payload", JSON.dump(value: "message")

    Redstream::Trimmer.new(expiry: 0, stream_name: "default", value: "value").run_once

    expect(redis.xlen(Redstream.stream_key_name("default"))).to eq(0)
  end

  it "shouldn't delete not yet expired messages" do
    redis.xadd Redstream.stream_key_name("default"), "*", "payload", JSON.dump(value: "message")

    trimmer = Redstream::Trimmer.new(expiry: 2, stream_name: "default", value: "value")

    thread = Thread.new do
      trimmer.run_once
    end

    sleep 1

    expect(redis.xlen(Redstream.stream_key_name("default"))).to eq(1)

    thread.join
  end
end

