
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Model do
  describe "after_save" do
    it "adds a delay message after save" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      time = Time.now

      product = Timecop.freeze(time) do
        create(:product)
      end

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(1)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").first[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "does not a delay message after save if there are no changes" do
      product = create(:product)

      expect { product.save }.not_to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
    end
  end

  describe "after_touch" do
    it "adds a delay message after touch" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)

      time = Time.now

      Timecop.freeze(time) do
        product.touch
      end

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(2)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end
  end

  describe "after_destroy" do
    it "adds a delay message after destroy" do
      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)

      product = create(:product)

      time = Time.now

      Timecop.freeze(time) do
        product.touch
      end

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(2)
      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end
  end

  describe "after_commit" do
    it "adds a queue message after commit" do
      expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(0)

      product = create(:product)

      expect(redis.xlen(Redstream.stream_key_name("products"))).to eq(1)
      expect(redis.xrange(Redstream.stream_key_name("products"), "-", "+").first[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "does not add a queue message after commit if there are no changes" do
      product = create(:product)

      expect { product.save }.not_to change { redis.xlen(Redstream.stream_key_name("products")) }
    end
  end
end

