require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Producer do
  describe "#queue" do
    it "adds a queue message for individual objects" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products")

      expect { Redstream::Producer.new.queue(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "deletes the delay message when given" do
      product = create(:product)

      producer = Redstream::Producer.new

      id = producer.delay(product)
      producer.queue(product, delay_message_id: id)

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)
    end
  end

  describe "#delay" do
    it "adds a delay message for individual objects" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new.delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
      expect(redis.xrange(stream_key_name, "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "resepects wait" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new(wait: 0).delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
    end
  end

  describe "#bulk" do
    it "adds bulk delay messages for scopes" do
      products = create_list(:product, 2)

      stream_key_name = Redstream.stream_key_name("products")

      expect(redis.xlen("#{stream_key_name}.delay")).to eq(0)

      Redstream::Producer.new.bulk(Product.all) do
        messages = redis.xrange("#{stream_key_name}.delay", "-", "+").last(2).map { |message| message[1] }

        expect(messages).to eq([
          { "payload" => JSON.dump(products[0].redstream_payload) },
          { "payload" => JSON.dump(products[1].redstream_payload) }
        ])
      end
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

    it "should respect wait for delay" do
      create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      products = create_list(:product, 2)

      expect { Redstream::Producer.new(wait: 0).bulk_delay(products) }.to change { redis.xlen(stream_key_name) }.by(2)
    end
  end
end
