
require "thread"

module Redstream
  class Consumer
    def initialize(redis: Redis.new, stream_name:, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @redis = redis
      @lock = Lock.new(redis: redis.dup, name: stream_name)
      @stream_name = stream_name
      @batch_size = batch_size
      @logger = logger
    end

    def run(&block)
      loop { run_once(&block) }
    end

    def run_once(&block)
      got_lock = @lock.acquire do
        offset = @redis.get(Redstream.offset_key_name(@stream_name))
        offset ||= "0-0"

        response = @redis.xread("COUNT", @batch_size, "BLOCK", 5_000, "STREAMS", Redstream.stream_key_name(@stream_name), offset)

        return unless response

        messages = response[0][1].map do |raw_message|
          Message.new(raw_message)
        end

        block.call(messages)

        offset = response[0][1].last[0]

        return unless offset

        commit offset
      end

      sleep(5) unless got_lock
    rescue => e
      @logger.error e

      sleep 5

      retry
    end

    def commit(offset)
      @redis.set Redstream.offset_key_name(@stream_name), offset
    end
  end
end

