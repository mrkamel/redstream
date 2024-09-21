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
    class Signal
      def initialize
        @mutex = Mutex.new
        @condition_variable = ConditionVariable.new
      end

      def wait(timeout)
        @mutex.synchronize { @condition_variable.wait(@mutex, timeout) }
      end

      def signal
        @condition_variable.signal
      end
    end

    def initialize(name:)
      @name = name
      @id = SecureRandom.hex
    end

    def acquire(&block)
      got_lock = get_lock

      if got_lock
        keep_lock(&block)
        release_lock
      end

      got_lock
    end

    def wait(timeout)
      @wait_redis ||= Redstream.connection_pool.with(&:dup)
      @wait_redis.brpop("#{Redstream.lock_key_name(@name)}.notify", timeout: timeout)
    end

    private

    def keep_lock(&block)
      stopped = false
      signal = Signal.new

      thread = Thread.new do
        until stopped
          Redstream.connection_pool.with do |redis|
            redis.expire(Redstream.lock_key_name(@name), 5)
          end

          signal.wait(3)
        end
      end

      block.call
    ensure
      stopped = true
      signal&.signal
      thread&.join
    end

    def get_lock
      @get_lock_script = <<~SCRIPT
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
      SCRIPT

      Redstream.connection_pool.with do |redis|
        redis.eval(@get_lock_script, argv: [Redstream.lock_key_name(@name), @id])
      end
    end

    def release_lock
      @release_lock_script = <<~SCRIPT
        local lock_key_name, id = ARGV[1], ARGV[2]

        local cur = redis.call('del', lock_key_name)

        if cur and cur == id then
          redis.call('del', lock_key_name)
        end

        redis.call('del', lock_key_name .. '.notify')
        redis.call('rpush', lock_key_name .. '.notify', '1')
      SCRIPT

      Redstream.connection_pool.with do |redis|
        redis.eval(@release_lock_script, argv: [Redstream.lock_key_name(@name), @id])
      end
    end
  end
end
