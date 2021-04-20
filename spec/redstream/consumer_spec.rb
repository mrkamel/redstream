require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Consumer do
  describe "#run_once" do
    it "doesn't call the block without messages" do
      called = false

      Redstream::Consumer.new(name: "consumer", stream_name: "products", batch_size: 5).run_once do |_batch|
        called = true
      end

      expect(called).to eq(false)
    end

    it "is mutually exclusive" do
      create :product

      calls = Concurrent::AtomicFixnum.new(0)

      threads = Array.new(2) do |_i|
        Thread.new do
          Redstream::Consumer.new(name: "consumer", stream_name: "products", batch_size: 5).run_once do |_batch|
            calls.increment

            sleep 1
          end
        end
      end

      threads.each(&:join)

      expect(calls.value).to eq(1)
    end

    it "is using the existing offset" do
      create_list(:product, 2)

      all_messages = redis.xrange(Redstream.stream_key_name("products"), "-", "+")

      expect(all_messages.size).to eq(2)

      redis.set(Redstream.offset_key_name(stream_name: "products", consumer_name: "consumer"), all_messages.first[0])

      messages = nil

      consumer = Redstream::Consumer.new(name: "consumer", stream_name: "products")

      consumer.run_once do |batch|
        messages = batch
      end

      expect(messages.size).to eq(1)
      expect(messages.first.raw_message).to eq(all_messages.last)
    end

    it "yields messages in batches" do
      create_list(:product, 15)

      consumer = Redstream::Consumer.new(name: "consumer", stream_name: "products", batch_size: 10)

      messages = nil

      consumer.run_once do |batch|
        messages = batch
      end

      expect(messages.size).to eq(10)

      consumer.run_once do |batch|
        messages = batch
      end

      expect(messages.size).to eq(5)
    end

    it "updates the offset" do
      create :product

      expect(redis.get(Redstream.offset_key_name(stream_name: "products", consumer_name: "consumer"))).to be(nil)

      all_messages = redis.xrange(Redstream.stream_key_name("products"), "-", "+")

      Redstream::Consumer.new(name: "consumer", stream_name: "products").run_once do
        # nothing
      end

      expect(redis.get(Redstream.offset_key_name(stream_name: "products", consumer_name: "consumer"))).to eq(all_messages.last[0])
    end
  end
end
