
module Redstream
  class Lock
    def initialize(redis: Redis.new, name:, value:)
      @redis = redis
      @name = name
      @value = value
    end

    def acquire(&block)
      got_lock = get_lock
      keep_lock(&block) if got_lock
      got_lock
    end

    private

    def keep_lock(&block)
      stop = false
      mutex = Mutex.new

      Thread.new do
        until mutex.synchronize { stop }
          @redis.expire(Redstream.lock_key_name(@name), 5)

          sleep 3
        end
      end

      block.call
    ensure
      mutex.synchronize do
        stop = true
      end
    end

    def get_lock
      @get_lock_script =<<-EOF
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

      @redis.eval(@get_lock_script, argv: [Redstream.lock_key_name(@name), @value])
    end
  end
end

