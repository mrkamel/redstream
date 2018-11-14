
module Redstream
  # The Redstream::Trimmer class is neccessary to clean up old messsages after
  # a configurable amount of time (e.g. 1 day) from a redis stream that will
  # otherwise fill up redis and finally bring redis down due to out of memory
  # issues.
  #
  # @example
  #   Redstream::Trimmer.new(expiry: 1.day, stream_name: "users").run

  class Trimmer
    # Initializes a new trimmer. Accepts an expiry param to specify when old
    # messages should be deleted from a stream, the actual stream name as well
    # as a logger for debug log messages.
    #
    # @param expiry [Fixnum, Float, ActiveSupport::Duration] Specifies the time
    #   to live for messages.
    # @param stream_name [String] The name of the stream that should be trimmed.
    #   Please note, that redstream adds a prefix to the redis keys. However,
    #   the stream_name param must be specified without any prefixes here. When
    #   using Redstream::Model, the stream name is the downcased, pluralized
    #   and underscored version of the model name. I.e., the stream name for a
    #   'User' model will be 'users'
    # @param logger [Logger] A logger used for debug messages

    def initialize(expiry:, stream_name:, logger: Logger.new("/dev/null"))
      @expiry = expiry
      @stream_name = stream_name
      @logger = logger
      @lock = Lock.new(name: "#{stream_name}-trimmer")
    end

    # Loops and blocks forever trimming messages from the specified redis
    # stream.

    def run
      loop { run_once }
    end

    # Runs the trimming a single time. You usually want to use the #run method
    # instead, which loops/blocks forever.

    def run_once
      got_lock = @lock.acquire do
        messages = Redstream.connection_pool.with do |redis|
          redis.xrange Redstream.stream_key_name(@stream_name), "-", (Time.now.to_f * 1000).to_i - @expiry, "COUNT", 1_000
        end
          
        return if messages.nil? || messages.empty?

        seconds_to_sleep = messages.first[0].to_f / 1_000 + @expiry.to_i - Time.now.to_f

        if seconds_to_sleep > 0
          sleep(seconds_to_sleep + 1)
        else
          expired_ids = messages.map(&:first).select { |id| id.to_f / 1_000 + @expiry.to_f - Time.now.to_f < 0 }

          Redstream.connection_pool.with { |redis| redis.xdel Redstream.stream_key_name(@stream_name), expired_ids }

          @logger.debug "Trimmed #{messages.size} messages from #{@stream_name}"
        end
      end

      sleep(5) unless got_lock
    end
  end
end

