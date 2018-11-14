
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
      @id = raw_message[0]
      @raw_message = raw_message
      @hash = Hash[raw_message[1].each_slice(2).to_a]
    end

    # Returns the message id, i.e. the redis message id consisting of a
    # timestamp plus sequence number.
    #
    # TODO: rename to #offset to avoid mixing it up with the model's id
    #
    # @returns [String] The message id

    def id
      @id
    end

    # Returns the parsed message payload as provided by the model's
    # #redstream_payload method. Check out Redstream::Model for more details.
    #
    # @return [Hash] The parsed payload

    def json_payload
      @json_payload ||= JSON.parse(@hash["payload"])
    end

    # As a redis stream message allows to specify fields,
    # this allows to retrieve the field content.
    #
    # @param key [String] The field name
    #
    # @returns The field content

    def [](key)
      @hash[key]
    end

    # Returns the raw message content as returned by redis.

    def raw_message
      @raw_message
    end
  end
end

