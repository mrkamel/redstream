require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Producer do
  describe "#queue" do
    it "adds a queue message for individual objects" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products")

      expect { Redstream::Producer.new.queue(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "uses a custom stream name when specified" do
      product = create(:product)

      allow(product).to receive(:redstream_name).and_return("stream-name")

      stream_key_name = Redstream.stream_key_name("stream-name")

      expect { Redstream::Producer.new.queue(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end
  end

  describe "#delay" do
    it "adds a delay message for individual objects" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new.delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "uses a custom stream name when specified" do
      product = create(:product)

      allow(product).to receive(:redstream_name).and_return("stream-name")

      stream_key_name = Redstream.stream_key_name("stream-name.delay")

      expect { Redstream::Producer.new.delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "respects wait" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new(wait: 0).delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
    end
  end

  describe "#bulk" do
    it "adds bulk delay messages for scopes" do
      products = create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("products")

      expect(redis.xlen("#{stream_key_name}.delay")).to eq(2)

      Redstream::Producer.new.bulk(Product.all) do
        expect(redis.xlen("#{stream_key_name}.delay")).to eq(4)

        messages = redis.xrange("#{stream_key_name}.delay", "-", "+").last(2).map { |message| message[1] }

        expect(messages).to eq([
          { "payload" => JSON.dump(products[0].redstream_payload) },
          { "payload" => JSON.dump(products[1].redstream_payload) }
        ])
      end
    end

    it "uses a custom stream name when specified" do
      allow_any_instance_of(Product).to receive(:redstream_name).and_return("stream-name")

      create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("stream-name")

      expect(redis.xlen(stream_key_name)).to eq(2)
      expect(redis.xlen("#{stream_key_name}.delay")).to eq(2)

      Redstream::Producer.new.bulk(Product.all) do
        # nothing
      end

      expect(redis.xlen(stream_key_name)).to eq(4)
      expect(redis.xlen("#{stream_key_name}.delay")).to eq(4)
    end

    it "adds bulk queue messages for scopes" do
      products = create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("products")

      expect do
        Redstream::Producer.new.bulk(Product.all) do
          # nothing
        end
      end.to change { redis.xlen(stream_key_name) }.by(2)

      messages = redis.xrange(stream_key_name, "-", "+").last(2).map { |message| message[1] }

      expect(messages).to eq([
        { "payload" => JSON.dump(products[0].redstream_payload) },
        { "payload" => JSON.dump(products[1].redstream_payload) }
      ])
    end
  end

  describe "#bulk_queue" do
    it "adds bulk queue messages for scopes" do
      products = create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("products")

      expect { Redstream::Producer.new.bulk_queue(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)

      messages = redis.xrange(stream_key_name, "-", "+").last(2).map { |message| message[1] }

      expect(messages).to eq([
        { "payload" => JSON.dump(products[0].redstream_payload) },
        { "payload" => JSON.dump(products[1].redstream_payload) }
      ])
    end

    it "uses a custom stream nameadds bulk queue messages for scopes" do
      allow_any_instance_of(Product).to receive(:redstream_name).and_return("stream-name")

      create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("stream-name")

      expect { Redstream::Producer.new.bulk_queue(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)
    end
  end

  describe "#bulk_delay" do
    it "adds bulk delay messages for scopes" do
      products = create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new.bulk_delay(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)

      messages = redis.xrange(stream_key_name, "-", "+").last(2).map { |message| message[1] }

      expect(messages).to eq([
        { "payload" => JSON.dump(products[0].redstream_payload) },
        { "payload" => JSON.dump(products[1].redstream_payload) }
      ])
    end

    it "uses a custom stream name when specified" do
      allow_any_instance_of(Product).to receive(:redstream_name).and_return("stream-name")

      create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("stream-name.delay")

      expect { Redstream::Producer.new.bulk_delay(Product.all) }.to change { redis.xlen(stream_key_name) }.by(2)
    end

    it "should respect wait for delay" do
      create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      products = create_list(:product, 2)

      expect { Redstream::Producer.new(wait: 0).bulk_delay(products) }.to change { redis.xlen(stream_key_name) }.by(2)
    end
  end
end
