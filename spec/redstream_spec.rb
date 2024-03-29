require File.expand_path("spec_helper", __dir__)

RSpec.describe Redstream do
  describe ".connection_pool=" do
    it "sets the connection pool" do
      begin
        connection_pool = Redstream.connection_pool

        Redstream.connection_pool = "pool"
        expect(Redstream.connection_pool).to eq("pool")
      ensure
        Redstream.connection_pool = connection_pool
      end
    end
  end

  describe ".connection_pool" do
    it "returns the connection pool" do
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
  end

  describe ".stream_size" do
    it "returns the stream's size" do
      expect(Redstream.stream_size("products")).to eq(0)

      redis.xadd("redstream:stream:products", { key: "value" })

      expect(Redstream.stream_size("products")).to eq(1)
    end
  end

  describe ".max_stream_id" do
    it "returns the stream's max id" do
      expect(Redstream.max_stream_id("products")).to be_nil

      _id1 = redis.xadd("redstream:stream:products", { key: "value" })
      id2 = redis.xadd("redstream:stream:products", { key: "value" })

      expect(Redstream.max_stream_id("products")).to eq(id2)
    end
  end

  describe ".max_consumer_id" do
    it "returns the consumer's max id" do
      expect(Redstream.max_consumer_id(stream_name: "products", consumer_name: "consumer")).to be_nil

      _id1 = redis.xadd("redstream:stream:products", { key: "value" })
      id2 = redis.xadd("redstream:stream:products", { key: "value" })

      Redstream::Consumer.new(name: "consumer", stream_name: "products").run_once do |messages|
        # nothing
      end

      expect(Redstream.max_consumer_id(stream_name: "products", consumer_name: "consumer")).to eq(id2)
    end
  end

  describe ".stream_key_name" do
    context "without namespace" do
      it "returns the stream key name" do
        expect(Redstream.stream_key_name("products")).to eq("redstream:stream:products")
      end
    end

    context "with namespace" do
      it "returns the stream key name" do
        begin
          Redstream.namespace = "namespace"
          expect(Redstream.stream_key_name("products")).to eq("namespace:redstream:stream:products")
        ensure
          Redstream.namespace = nil
        end
      end
    end
  end

  describe ".offset_key_name" do
    context "without namespace" do
      it "returns the offset key name" do
        expect(Redstream.offset_key_name(stream_name: "stream", consumer_name: "consumer")).to eq("redstream:offset:stream:consumer")
      end
    end

    context "with namespace" do
      it "returns the offset key name" do
        begin
          Redstream.namespace = "namespace"
          expect(Redstream.offset_key_name(stream_name: "stream", consumer_name: "consumer")).to eq("namespace:redstream:offset:stream:consumer")
        ensure
          Redstream.namespace = nil
        end
      end
    end
  end

  describe ".lock_key_name" do
    context "without namespace" do
      it "returns the lock key name" do
        expect(Redstream.lock_key_name("name")).to eq("redstream:lock:name")
      end
    end

    context "with namespace" do
      it "returns the lock key name" do
        begin
          Redstream.namespace = "namespace"
          expect(Redstream.lock_key_name("name")).to eq("namespace:redstream:lock:name")
        ensure
          Redstream.namespace = nil
        end
      end
    end
  end
end
