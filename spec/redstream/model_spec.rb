require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Model do
  describe "after_save" do
    it "adds a delay message after_save" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(1)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").first[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue message after_save on commit" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      create(:product)

      expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(1)
    end

    it "does not add a delay message after_save if there are no changes" do
      product = create(:product)

      expect { product.save }.not_to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
    end

    it "does not add a queue messages after_save on commit if there are no changes" do
      product = create(:product)

      expect { product.save }.not_to change { redis.xlen(Redstream.stream_key_name("products")) }
    end
  end

  describe "after_touch" do
    it "adds a delay message after touch" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)
      product.touch

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(2)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue message after touch commit" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)
      product.touch

      expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(2)
    end
  end

  describe "after_destroy" do
    it "adds a delay message after destroy" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)
      product.destroy

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(2)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue messages after destroy on commit" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)
      product.destroy

      expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(2)
    end
  end
end
