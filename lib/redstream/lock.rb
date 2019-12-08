require "securerandom"

module Redstream
  # @api private
  #
  # As the name suggests, the Redstream::Lock class implements a redis based
  # locking mechanism. It atomically (lua script) gets/sets the lock key and
  # updates its expire timeout, in case it currently holds the lock. Moreover,
  # once it got the lock, it tries to keep it by updating the lock expire
  # timeout from within a thread every 3 seconds.
  #
  # @example
  #   lock = Redstream::Lock.new(name: "user_stream_lock")
  #
  #   loop do
  #     got_lock = lock.acquire do
  #       # ...
  #     end
  #
  #     sleep(5) unless got_lock
  #   end

  class Lock
    def initialize(name:)
      @name = name
      @id = SecureRandom.hex
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
          Redstream.connection_pool.with { |redis| redis.expire(Redstream.lock_key_name(@name), 5) }

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
      @get_lock_script = <<~GET_LOCK_SCRIPT
        local lock_key_name, id = ARGV[1], ARGV[2]

        local cur = redis.call('get', lock_key_name)

        if not cur then
          redis.call('setex', lock_key_name, 5, id)

          return true
        elseif cur == id then
          redis.call('expire', lock_key_name, 5)

          return true
        end

        return false
      GET_LOCK_SCRIPT

      Redstream.connection_pool.with { |redis| redis.eval(@get_lock_script, argv: [Redstream.lock_key_name(@name), @id]) }
    end
  end
end
