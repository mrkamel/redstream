require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Delayer do
  describe "#run_once" do
    it "copies expired messages to their target streams" do
      redis.xadd Redstream.stream_key_name("target.delay"), payload: JSON.dump(value: "message")

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(0)

      Redstream::Delayer.new(stream_name: "target", delay: 0).run_once

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
      expect(redis.xrange(Redstream.stream_key_name("target")).last[1]).to eq("payload" => JSON.dump(value: "message"))
    end

    it "delivers and commits before falling asleep" do
      redis.xadd Redstream.stream_key_name("target.delay"), payload: JSON.dump(value: "message")
      sleep 3
      redis.xadd Redstream.stream_key_name("target.delay"), payload: JSON.dump(value: "message")

      thread = Thread.new do
        Redstream::Delayer.new(stream_name: "target", delay: 1).run_once
      end

      sleep 1

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
      expect(redis.get(Redstream.offset_key_name(stream_name: "target.delay", consumer_name: "delayer"))).not_to be_nil

      thread.join

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(2)
    end

    it "does not copy not yet expired messages" do
      redis.xadd Redstream.stream_key_name("target.delay"), payload: JSON.dump(value: "message")

      thread = Thread.new do
        Redstream::Delayer.new(stream_name: "target", delay: 2).run_once
      end

      sleep 1

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(0)

      thread.join

      expect(redis.xlen(Redstream.stream_key_name("target"))).to eq(1)
    end

    it "logs an error and sleeps when e.g. redis can not be reached" do
      redis.xadd Redstream.stream_key_name("target.delay"), payload: JSON.dump(value: "message")

      allow_any_instance_of(Redis).to receive(:xadd).and_raise(Redis::ConnectionError)

      logger = Logger.new("/dev/null")
      allow(logger).to receive(:error)

      delayer = Redstream::Delayer.new(stream_name: "target", delay: 0, logger: logger)
      allow(delayer).to receive(:sleep)

      delayer.run_once

      expect(logger).to have_received(:error).with(Redis::ConnectionError)
      expect(delayer).to have_received(:sleep).with(5)
    end
  end
end
