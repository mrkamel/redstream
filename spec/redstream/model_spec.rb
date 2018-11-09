
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Model do
  it "should delay after save" do
    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(0)

    time = Time.now

    product = Timecop.freeze(time) do
      create(:product)
    end

    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(1)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("delay"), "-", "+")[0][1])

    expect(message["payload"]).to eq(JSON.dump(product.redstream_payload))
    expect(message["stream_name"]).to eq("products")
  end

  it "should delay after touch" do
    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(0)

    product = create(:product)

    time = Time.now

    Timecop.freeze(time) do
      product.touch
    end

    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(2)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("delay"), "-", "+")[1][1])

    expect(message["payload"]).to eq(JSON.dump(product.redstream_payload))
    expect(message["stream_name"]).to eq("products")
  end

  it "should delay after destroy" do
    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(0)

    product = create(:product)

    time = Time.now

    Timecop.freeze(time) do
      product.touch
    end

    expect(redis.xlen(Redstream.stream_key_name("delay"))).to eq(2)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("delay"), "-", "+")[1][1])

    expect(message["payload"]).to eq(JSON.dump(product.redstream_payload))
    expect(message["stream_name"]).to eq("products")
  end

  it "should queue after commit" do
    expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(0)

    product = create(:product)

    expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(1)

    message = array_to_hash(redis.xrange(Redstream.stream_key_name("products"), "-", "+")[0][1])

    expect(message["payload"]).to eq(JSON.dump(product.redstream_payload))
  end
end

