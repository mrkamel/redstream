
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Producer do
  let(:producer) { Redstream::Producer.new }

  it "should queue messages for individual objects" do
    product = create(:product)

    expect { producer.queue(product) }.to change { redis.xlen(Redstream.stream_key_name("products")) }.by(1)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("products"), "-", "+").last[1])

    expect(message).to eq("payload" => JSON.dump(product.redstream_payload))
  end

  it "should delay messages for individual objects" do
    product = create(:product)

    expect { producer.delay(product) }.to change { redis.xlen(Redstream.stream_key_name("delay")) }.by(1)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("delay"), "-", "+").last[1])

    expect(message).to eq("payload" => JSON.dump(product.redstream_payload), "stream_name" => "products")
  end

  it "should bulk queue messages for scopes" do
    products = create_list(:product, 2)

    expect { producer.bulk_queue(Product.all) }.to change { redis.xlen(Redstream.stream_key_name("products")) }.by(2)

    messages = redis.xrange(Redstream.stream_key_name("products"), "-", "+").last(2).map { |message| array_to_hash message[1] }

    expect(messages).to eq([
      { "payload" => JSON.dump(products[0].redstream_payload) },
      { "payload" => JSON.dump(products[1].redstream_payload) }
    ])
  end

  it "should bulk delay messages for scopes" do
    products = create_list(:product, 2)

    expect { producer.bulk_delay(Product.all) }.to change { redis.xlen(Redstream.stream_key_name("delay")) }.by(2)

    messages = redis.xrange(Redstream.stream_key_name("delay"), "-", "+").last(2).map { |message| array_to_hash message[1] }

    expect(messages).to eq([
      { "payload" => JSON.dump(products[0].redstream_payload), "stream_name" => "products" },
      { "payload" => JSON.dump(products[1].redstream_payload), "stream_name" => "products" }
    ])
  end
end

