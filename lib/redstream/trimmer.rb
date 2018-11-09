
module Redstream
  class Trimmer
    def initialize(redis: Redis.new, expiry:, stream_name:, logger: Logger.new("/dev/null"))
      @redis = redis
      @expiry = expiry
      @stream_name = stream_name
      @logger = logger
      @lock = Lock.new(redis: redis.dup, name: "#{stream_name}-trimmer")
    end

    def run
      loop { run_once }
    end

    def run_once
      got_lock = @lock.acquire do
        messages = @redis.xrange(Redstream.stream_key_name(@stream_name), "-", (Time.now.to_f * 1000).to_i - @expiry, "COUNT", 1_000)
          
        return if messages.nil? || messages.empty?

        seconds_to_sleep = messages.first[0].to_f / 1_000 + @expiry.to_i - Time.now.to_f

        if seconds_to_sleep > 0
          sleep(seconds_to_sleep + 1)
        else
          expired_ids = messages.map(&:first).select { |id| id.to_f / 1_000 + @expiry.to_f - Time.now.to_f < 0 }

          @redis.xdel Redstream.stream_key_name(@stream_name), expired_ids

          @logger.debug "Trimmed #{messages.size} messages from #{@stream_name}"
        end
      end

      sleep(5) unless got_lock
    end
  end
end

