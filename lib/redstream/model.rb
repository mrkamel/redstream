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
    IVAR_DELAY_MESSAGE_ID = :@__redstream_delay_message_id__

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
        after_save    { |object| instance_variable_set(IVAR_DELAY_MESSAGE_ID, producer.delay(object)) if object.saved_changes.present? }
        after_touch   { |object| instance_variable_set(IVAR_DELAY_MESSAGE_ID, producer.delay(object)) }
        after_destroy { |object| instance_variable_set(IVAR_DELAY_MESSAGE_ID, producer.delay(object)) }

        after_commit(on: [:create, :update]) do |object|
          if object.saved_changes.present?
            producer.queue(object, delay_message_id: instance_variable_get(IVAR_DELAY_MESSAGE_ID))
            instance_variable_set(IVAR_DELAY_MESSAGE_ID, nil)
          end
        end

        after_commit(on: :destroy) do |object|
          producer.queue(object, delay_message_id: instance_variable_get(IVAR_DELAY_MESSAGE_ID))
          instance_variable_set(IVAR_DELAY_MESSAGE_ID, nil)
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
  end
end
