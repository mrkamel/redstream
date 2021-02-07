require "thread"

module Redstream
  # The Redstream::Consumer class to read messages from a specified redis
  # stream in batches.
  #
  # @example
  #   Redstream::Consumer.new(name: "user_indexer", stream_name: "users").run do |messages|
  #     # ...
  #   end

  class Consumer
    # Initializes a new consumer instance. Please note that you can have
    # multiple consumers per stream, by specifying different names.
    #
    # @param name [String] The consumer name. The name is used for locking
    # @param stream_name [String] The name of the redis stream. Please note
    #   that redstream adds a prefix to the redis keys. However, the
    #   stream_name param must be specified without any prefixes here.
    #   When using Redstream::Model, the stream name is the downcased,
    #   pluralized and underscored version of the model name. I.e., the
    #   stream name for a 'User' model will be 'users'
    # @param batch_size [Fixnum] The desired batch size, that is the number
    #   of messages yielded at max. More concretely, the number of messages
    #   yielded may be lower the batch_size, but not higher
    # @param logger [Logger] The logger used for error logging

    def initialize(name:, stream_name:, batch_size: 1_000, logger: Logger.new("/dev/null"))
      @name = name
      @stream_name = stream_name
      @batch_size = batch_size
      @logger = logger
      @redis = Redstream.connection_pool.with(&:dup)
      @lock = Lock.new(name: "consumer:#{@stream_name}:#{@name}")
    end

    # Returns its maximum committed id, i.e. the consumer's offset.
    #
    # @return [String, nil] The committed id, or nil

    def max_committed_id
      @redis.get Redstream.offset_key_name(stream_name: @stream_name, consumer_name: @name)
    end

    # Loops and thus blocks forever while reading messages from the specified
    # stream and yielding them in batches.
    #
    # @example
    #   consumer.run do |messages|
    #     # ...
    #   end

    def run(&block)
      loop { run_once(&block) }
    end

    # Reads a single batch from the specified stream and yields it. You usually
    # want to use the #run method instead, which loops/blocks forever.
    #
    # @example
    #   consumer.run_once do |messages|
    #     # ...
    #   end

    def run_once(&block)
      got_lock = @lock.acquire do
        offset = @redis.get(Redstream.offset_key_name(stream_name: @stream_name, consumer_name: @name))
        offset ||= "0-0"

        stream_key_name = Redstream.stream_key_name(@stream_name)

        response = begin
          @redis.xread(stream_key_name, offset, count: @batch_size, block: 5_000)
        rescue Redis::TimeoutError
          nil
        end

        return if response.nil? || response[stream_key_name].nil? || response[stream_key_name].empty?

        new_offset = response[stream_key_name].last[0]

        raise(InvalidMessageID, "Invalid message ID #{new_offset.inspect}") unless new_offset

        messages = response[stream_key_name].map do |raw_message|
          Message.new(raw_message)
        end

        block.call(messages)

        commit(new_offset)
      end

      sleep(5) unless got_lock
    rescue StandardError => e
      @logger.error(e)

      sleep(5)
    end

    # @api private
    #
    # Commits the specified offset/ID as the maximum ID already read, such that
    # subsequent read calls will use this offset/ID as a starting point.
    #
    # @param offset [String] The offset/ID to commit

    def commit(offset)
      @redis.set Redstream.offset_key_name(stream_name: @stream_name, consumer_name: @name), offset
    end
  end
end
