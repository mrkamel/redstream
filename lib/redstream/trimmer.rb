
module Redstream
  # The Redstream::Trimmer class is neccessary to clean up messsages after all
  # consumers have successfully processed and committed them. Otherwise they
  # would fill up redis and finally bring redis down due to out of memory
  # issues. The Trimmer will sleep for the specified interval in case there is
  # nothing to trim. Please note that you must pass an array containing all
  # consumer names reading from the stream which is about to be trimmed.
  # Otherwise the Trimmer could trim messages from the stream before all
  # consumers received the respective messages.
  #
  # @example
  #   trimmer = Redstream::Trimmer.new(
  #     interval: 30,
  #     stream_name: "users",
  #     consumer_names: ["indexer", "cacher"]
  #   )
  #
  #   trimmer.run

  class Trimmer
    # Initializes a new trimmer. Accepts an interval to sleep for in case there
    # is nothing to trim, the actual stream name, the consumer names as well as
    # a logger for debug log messages.
    #
    # @param interval [Fixnum, Float] Specifies a time to sleep in case there is
    #   nothing to trim.
    # @param stream_name [String] The name of the stream that should be trimmed.
    #   Please note, that redstream adds a prefix to the redis keys. However,
    #   the stream_name param must be specified without any prefixes here. When
    #   using Redstream::Model, the stream name is the downcased, pluralized
    #   and underscored version of the model name. I.e., the stream name for a
    #   'User' model will be 'users'
    # @params consumer_names [Array] The list of all consumers reading from the
    #   specified stream
    # @param logger [Logger] A logger used for debug messages

    def initialize(interval:, stream_name:, consumer_names:, logger: Logger.new("/dev/null"))
      @interval = interval
      @stream_name = stream_name
      @consumer_names = consumer_names
      @logger = logger
      @lock = Lock.new(name: "trimmer:#{stream_name}")
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
        min_committed_id = Redstream.connection_pool.with do |redis|
          offset_key_names = @consumer_names.map do |consumer_name|
            Redstream.offset_key_name(stream_name: @stream_name, consumer_name: consumer_name)
          end

          redis.mget(offset_key_names).map(&:to_s).reject(&:empty?).min
        end

        return sleep(@interval) unless min_committed_id

        loop do
          messages = Redstream.connection_pool.with do |redis|
            redis.xrange Redstream.stream_key_name(@stream_name), "-", min_committed_id, "COUNT", 1_000
          end

          return sleep(@interval) if messages.nil? || messages.empty?

          Redstream.connection_pool.with { |redis| redis.xdel Redstream.stream_key_name(@stream_name), messages.map(&:first) }

          @logger.debug "Trimmed #{messages.size} messages from #{@stream_name}"
        end
      end

      sleep(5) unless got_lock
    end
  end
end

