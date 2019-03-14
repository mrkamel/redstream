
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Producer do
  it "should queue messages for individual objects" do
    product = create(:product)

    stream_key_name = Redstream.stream_key_name("products")

    expect { Redstream::Producer.new.queue(product) }.to change { redis.xlen(stream_key_name) }.by(1)
    expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
  end

  it "should delay messages for individual objects" do
    product = create(:product)

    stream_key_name = Redstream.stream_key_name("products.delay")

    expect { Redstream::Producer.new.delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
    expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
  end

  it "should bulk queue messages for scopes" do
    products = create_list(:product, 2)

    stream_key_name = Redstream.stream_key_name("products")

    expect { Redstream::Producer.new.bulk_queue(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)

    messages = redis.xrange(stream_key_name, "-", "+").last(2).map { |message| message[1] }

    expect(messages).to eq([
      { "payload" => JSON.dump(products[0].redstream_payload) },
      { "payload" => JSON.dump(products[1].redstream_payload) }
    ])
  end

  it "should bulk delay messages for scopes" do
    products = create_list(:product, 2)

    stream_key_name = Redstream.stream_key_name("products.delay")

    expect { Redstream::Producer.new.bulk_delay(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)

    messages = redis.xrange(stream_key_name, "-", "+").last(2).map { |message| message[1] }

    expect(messages).to eq([
      { "payload" => JSON.dump(products[0].redstream_payload) },
      { "payload" => JSON.dump(products[1].redstream_payload) }
    ])
  end

  it "should resepect wait for delay" do
    product = create(:product)

    stream_key_name = Redstream.stream_key_name("products.delay")

    expect { Redstream::Producer.new(wait: 0).delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)

    products = create_list(:product, 2)

    expect { Redstream::Producer.new(wait: 0).bulk_delay(products) }.to change { redis.xlen(stream_key_name) }.by(2)
  end
end

