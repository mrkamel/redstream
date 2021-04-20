require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Producer do
  describe "#queue" do
    it "adds a queue message for individual objects" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products")

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

    it "resepects wait" do
      product = create(:product)

      stream_key_name = Redstream.stream_key_name("products.delay")

      expect { Redstream::Producer.new(wait: 0).delay(product) }.to change { redis.xlen(stream_key_name) }.by(1)
    end
  end

  describe "#destroy" do
    it "deletes the delay message for the object" do
      product = create(:product)

      producer = Redstream::Producer.new

      id = producer.delay(product)
      producer.delete(product, id)

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)
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

    it "deletes the delay messages after the queue messages have been sent" do
      products = create_list(:product, 2)
      producer = Redstream::Producer.new

      other_id = producer.delay(create(:product))

      producer.bulk_queue(Product.all) do
        expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(2)
      end

      expect(redis.xrange(Redstream.stream_key_name("products.delay"), '-', '+').map(&:first)).to eq([other_id])
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

  describe "#bulk_delete" do
    it "deletes delay messages for scopes" do
      products = create_list(:product, 2)
      producer = Redstream::Producer.new

      other_id = producer.delay(create(:product))

      ids = producer.bulk_delay(products)
      producer.bulk_delete(products, ids)

      expect(redis.xrange(Redstream.stream_key_name("products.delay"), '-', '+').map(&:first)).to eq([other_id])
    end
  end
end
