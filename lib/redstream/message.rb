
module Redstream
  # The Redstream::Message class wraps a raw redis stream message to allow hash
  # and id/offset access as well as convenient parsing of the json payload.

  class Message
    def initialize(raw_message)
      @id = raw_message[0]
      @raw_message = raw_message
      @hash = Hash[raw_message[1].each_slice(2).to_a]
    end

    def id
      @id
    end

    def json_payload
      @json_payload ||= JSON.parse(@hash["payload"])
    end

    def [](key)
      @hash[key]
    end

    def raw_message
      @raw_message
    end
  end
end

