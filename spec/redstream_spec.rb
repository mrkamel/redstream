
require File.expand_path("spec_helper", __dir__)

RSpec.describe Redstream do
  it "should allow seting the connection pool" do
    begin
      connection_pool = Redstream.connection_pool

      Redstream.connection_pool = "pool"
      expect(Redstream.connection_pool).to eq("pool")
    ensure
      Redstream.connection_pool = connection_pool
    end
  end

  it "should allow getting the connection pool" do
    begin
      connection_pool = Redstream.connection_pool

      Redstream.connection_pool = nil
      expect(Redstream.connection_pool).to be_a(ConnectionPool)

      Redstream.connection_pool = "pool"
      expect(Redstream.connection_pool).to eq("pool")
    ensure
      Redstream.connection_pool = connection_pool
    end
  end

  it "should return a stream's max id" do
    expect(Redstream.max_id("products")).to be_nil

    id1 = redis.xadd("redstream:stream:products", "*", "key", "value")
    id2 = redis.xadd("redstream:stream:products", "*", "key", "value")

    expect(Redstream.max_id("products")).to eq(id2)
  end

  it "should return a consumer's offset" do
    expect(Redstream.consumer_offset("product_consumer")).to be_nil

    id1 = redis.xadd("redstream:stream:products", "*", "key", "value")
    id2 = redis.xadd("redstream:stream:products", "*", "key", "value")

    Redstream::Consumer.new(name: "product_consumer", stream_name: "products").run_once do |messages|
      # nothing
    end

    expect(Redstream.consumer_offset("product_consumer")).to eq(id2)
  end

  it "should generate a stream key name" do
    expect(Redstream.stream_key_name("products")).to eq("redstream:stream:products")
  end

  it "should generate a offset key name" do
    expect(Redstream.offset_key_name("consumer")).to eq("redstream:offset:consumer")
  end

  it "should generate a lock key name" do
    expect(Redstream.lock_key_name("name")).to eq("redstream:lock:name")
  end
end

