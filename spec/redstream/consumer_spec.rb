
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Consumer do
  it "should be mutually exclusive" do
    create :product

    calls = Concurrent::AtomicFixnum.new(0)

    threads = Array.new(2) do |i|
      Thread.new do
        Redstream::Consumer.new(name: "product_consumer", stream_name: "products", batch_size: 5).run_once do |batch|
          calls.increment

          sleep 1
        end
      end
    end

    threads.each(&:join)

    expect(calls.value).to eq(1)
  end

  it "should use an existing offset" do
    create_list(:product, 2)

    all_messages = redis.xrange(Redstream.stream_key_name("products"), "-", "+")

    expect(all_messages.size).to eq(2)

    redis.set(Redstream.offset_key_name("product_consumer"), all_messages[0][0])

    messages = nil

    consumer = Redstream::Consumer.new(name: "product_consumer", stream_name: "products")

    consumer.run_once do |batch|
      messages = batch
    end

    expect(messages.size).to eq(1)
    expect(messages[0].raw_message).to eq(all_messages[1])
  end

  it "should yield messages in batches" do
    products = create_list(:product, 15)

    consumer = Redstream::Consumer.new(name: "product_consumer", stream_name: "products", batch_size: 10)

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

  it "should update the offset" do
    create :product

    expect(redis.get(Redstream.offset_key_name("product_consumer"))).to be(nil)

    all_messages = redis.xrange(Redstream.stream_key_name("products"), "-", "+")

    Redstream::Consumer.new(name: "product_consumer", stream_name: "products").run_once {}

    expect(redis.get(Redstream.offset_key_name("product_consumer"))).to eq(all_messages.last[0])
  end
end

