require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Model do
  describe "after_save" do
    it "adds a delay message after_save" do
      Product.transaction do
        expect { create(:product) }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
      end
    end

    it "assigns the delay message id" do
      Product.transaction do
        expect(create(:product).instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_present
      end
    end

    it "adds the correct payload for the delay message" do
      Product.transaction do
        product = create(:product)

        expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").first[1]).to eq("payload" => JSON.dump(product.redstream_payload))
      end
    end

    it "adds a queue message after_save on commit" do
      expect { create(:product) }.to change { redis.xlen(Redstream.stream_key_name("products")) }
    end

    it "deletes the delay message on commit" do
      product = create(:product)

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)
      expect(product.instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_nil
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

      Product.transaction do
        expect { product.touch }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
      end
    end

    it "assigns the delay message id" do
      product = create(:product)

      Product.transaction do
        product.touch

        expect(product.instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_present
      end
    end

    it "sets the correct payload for the delay message" do
      product = create(:product)

      Product.transaction do
        product.touch

        expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
      end
    end

    it "adds a queue message after touch on commit" do
      product = create(:product)

      expect { product.touch }.to change { redis.xlen(Redstream.stream_key_name("products")) }
    end

    it "deletes the delay message after touch on commit" do
      product = create(:product)
      product.touch

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)
      expect(product.instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_nil
    end
  end

  describe "after_destroy" do
    it "adds a delay message after destroy" do
      product = create(:product)

      Product.transaction do
        expect { product.destroy }.to change { redis.xlen(Redstream.stream_key_name("products.delay")) }
      end
    end

    it "assigns the delay message id" do
      product = create(:product)

      Product.transaction do
        product.destroy

        expect(product.instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_present
      end
    end

    it "sets the correct payload for the delay message" do
      product = create(:product)

      Product.transaction do
        product.destroy

        expect(redis.xrange(Redstream.stream_key_name("products.delay"), "-", "+").last[1]).to eq("payload" => JSON.dump(product.redstream_payload))
      end
    end

    it "adds a queue messages after destroy on commit" do
      product = create(:product)

      expect { product.destroy }.to change { redis.xlen(Redstream.stream_key_name("products")) }.by(1)
    end

    it "deletes the delay message after destroy on commit" do
      product = create(:product)
      product.destroy

      expect(redis.xlen(Redstream.stream_key_name("products.delay"))).to eq(0)
      expect(product.instance_variable_get(Redstream::Model::IVAR_DELAY_MESSAGE_ID)).to be_nil
    end
  end
end
