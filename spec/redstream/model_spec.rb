require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Model do
  describe "after_save" do
    it "adds a delay message after_save" do
      expect { create(:product) }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
    end

    it "adds the correct payload for the delay message" do
      product = create(:product)

      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").first[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue message after_save on commit" do
      expect { create(:product) }.to change { redis.xlen(Redstream.stream_key_name("products")) }
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
      product = create(:product)

      expect { product.touch }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
    end

    it "sets the correct payload for the delay message" do
      product = create(:product)
      product.touch

      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue message after touch commit" do
      product = create(:product)

      expect { product.touch }.to change { redis.xlen(Redstream.stream_key_name("products")) }
    end
  end

  describe "after_destroy" do
    it "adds a delay message after destroy" do
      product = create(:product)

      expect { product.destroy }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
    end

    it "sets the correct payload for the delay message" do
      product = create(:product)
      product.destroy

      expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
    end

    it "adds a queue messages after destroy on commit" do
      product = create(:product)

      expect { product.destroy }.to change { redis.xlen(Redstream.stream_key_name("products")) }.by(1)
    end
  end
end
