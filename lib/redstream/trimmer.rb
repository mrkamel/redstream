
module Redstream
  class Trimmer
    def initialize(redis: Redis.new, expiry:, stream_names:, logger: Logger.new("/dev/null"))
      @redis = redis
      @expiry = expiry
      @stream_names = stream_names
      @logger = logger
    end

    def run
      loop do
        run_once

        sleep @expiry
      end
    end

    def run_once
      @stream_names.each do |stream_name|
        trim stream_name
      end
    end

    private

    def trim(stream_name)
      loop do
        messages = @redis.xrange(Redstream.stream_key_name(stream_name), "-", (Time.now.to_f * 1000).to_i - @expiry, "COUNT", 1_000)

        return if messages.nil? || messages.empty?

        @redis.xdel Redstream.stream_key_name(stream_name), messages.map(&:first)

        @logger.debug "Trimmed #{messages.size} messages from #{stream_name}"
      end
    end
  end
end

