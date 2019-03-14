
module Redstream
  # The Redstream::Message class wraps a raw redis stream message to allow hash
  # and id/offset access as well as convenient parsing of the json payload.

  class Message
    # @api private
    #
    # Initializes the message.
    #
    # @param raw_message [Array] The raw message as returned by redis

    def initialize(raw_message)
      @message_id = raw_message[0]
      @raw_message = raw_message
    end

    # Returns the message id, i.e. the redis message id consisting of a
    # timestamp plus sequence number.
    #
    # @returns [String] The message id

    def message_id
      @message_id
    end

    # Returns the parsed message payload as provided by the model's
    # #redstream_payload method. Check out Redstream::Model for more details.
    #
    # @return [Hash] The parsed payload

    def payload
      @payload ||= JSON.parse(fields["payload"])
    end

    # As a redis stream message allows to specify fields,
    # this allows to retrieve the fields as a hash.
    #
    # @returns The fields hash

    def fields
      @fields ||= @raw_message[1]
    end

    # Returns the raw message content as returned by redis.

    def raw_message
      @raw_message
    end
  end
end

