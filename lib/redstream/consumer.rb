
require "thread"

module Redstream
  class Consumer
    class WrappedMessage
      def initialize(id, message)
        @id = id
        @message = message
      end

      def id
        @id
      end

      def payload
        @payload ||= JSON.parse(@message["payload"])
      end

      def [](key)
        @message[key]
      end
    end

    def initialize(redis: Redis.new, stream_name:, value:, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @redis = redis
      @lock_redis = redis.dup
      @stream_name = stream_name
      @batch_size = batch_size
      @value = value
      @logger = logger
    end

    def run(&block)
      loop { run_once(&block) }
    end

    def run_once(&block)
      unless acquire_lock
        sleep 5

        return
      end

      keep_lock do
        offset = @redis.get(Redstream.offset_key_name(@stream_name))
        offset ||= "0-0"

        response = @redis.xread("COUNT", @batch_size, "BLOCK", 5_000, "STREAMS", Redstream.stream_key_name(@stream_name), offset)

        return unless response

        messages = response[0][1].map do |id, message|
          WrappedMessage.new(id, Hash[message.each_slice(2).to_a])
        end

        block.call(messages)

        offset = response[0][1].last[0]

        return unless offset

        commit offset
      end
    rescue => e
      @logger.error e

      sleep 5

      retry
    end

    def commit(offset)
      @redis.set Redstream.offset_key_name(@stream_name), offset
    end

    private

    def keep_lock
      stop = false
      mutex = Mutex.new

      Thread.new do
        until mutex.synchronize { stop }
          @lock_redis.expire(Redstream.lock_key_name(@stream_name), 5)

          sleep 3
        end
      end

      yield
    ensure
      mutex.synchronize do
        stop = true
      end
    end

    def acquire_lock
      @acquire_lock_script =<<-EOF
        local lock_key_name, value = ARGV[1], ARGV[2]

        local cur = redis.call('get', lock_key_name)

        if not cur then
          redis.call('setex', lock_key_name, 5, value)

          return true
        elseif cur == value then
          redis.call('expire', lock_key_name, 5)

          return true
        end

        return false
      EOF

      return @redis.eval(@acquire_lock_script, argv: [Redstream.lock_key_name(@stream_name), @value])
    end
  end
end

