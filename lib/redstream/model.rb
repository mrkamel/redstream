module Redstream
  # Include Redstream::Model in your model to stream the model's updates via
  # redis streams.
  #
  # @example
  #   class User < ActiveRecord::Base
  #     include Redstream::Model
  #
  #     # ...
  #
  #     redstream_callbacks
  #   end

  module Model
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Adds after_save, after_touch, after_destroy and, most importantly,
      # after_commit callbacks. after_save, after_touch and after_destroy write
      # a delay message to a delay stream. The delay messages are exactly like
      # other messages, but will be read and replayed by a Redstream::Delayer
      # only after a certain amount of time has passed (5 minutes usually) to
      # fix potential inconsistencies which result from network or other issues
      # in between database commit and the rails after_commit callback.
      #
      # @param producer [Redstream::Producer] A Redstream::Producer that is
      #   responsible for writing to a redis stream

      def redstream_callbacks(producer: Producer.new)
        after_save    { |object| producer.delay(object) if object.saved_changes.present? }
        after_touch   { |object| producer.delay(object) }
        after_destroy { |object| producer.delay(object) }

        after_commit(on: [:create, :update]) do |object|
          producer.queue(object) if object.saved_changes.present?
        end

        after_commit(on: :destroy) do |object|
          producer.queue(object)
        end
      end

      def redstream_name
        name.pluralize.underscore
      end
    end

    # Override to customize the message payload. By default, the payload
    # consists of the record id only (see example 1).
    #
    # @example Default
    #   def redstream_payload
    #     { id: id }
    #   end

    def redstream_payload
      { id: id }
    end

    # Override to customize the stream name. By default, the stream name
    # is determined by the class name. If you override the instance method,
    # please also override the class method.
    #
    # @example Sharding
    #   class Product
    #     include Redstream::Model
    #
    #     NUM_SHARDS = 4
    #
    #     def redstream_name
    #       self.class.redstream_name(Digest::SHA1.hexdigest(id.to_s)[0, 4].to_i(16) % NUM_SHARDS)
    #     end
    #
    #     def self.redstream_name(shard)
    #       "products-#{shard}
    #     end
    #   end

    def redstream_name
      self.class.redstream_name
    end
  end
end
