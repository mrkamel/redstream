module Redstream
  # The Redstream::Delayer class is responsible for reading messages from
  # special delay streams which are used to fix inconsistencies resulting from
  # network or other issues in between after_save and after_commit callbacks.
  # To be able to fix such issues, delay messages will be added to a delay
  # stream within an after_save callback. The delay messages aren't fetched
  # immediately, but e.g. 5 minutes later, such that we can be sure that the
  # database transaction is committed or has been rolled back, but is no longer
  # running.
  #
  # @example
  #   Redstream::Delayer.new(stream_name: "users", delay: 5.minutes, logger: Logger.new(STDOUT)).run

  class Delayer
    # Initializes the delayer for the specified stream name and delay.
    #
    # @param stream_name [String] The stream name. Please note, that redstream
    #   adds a prefix to the redis keys. However, the stream_name param must
    #   be specified without any prefixes here. When using Redstream::Model,
    #   the stream name is the downcased, pluralized and underscored version
    #   of the model name. I.e., the stream name for a 'User' model will be
    #   'users'
    # @param delay [Fixnum, Float, ActiveSupport::Duration] The delay, i.e.
    #   the age a message must have before processing it.
    # @param logger [Logger] The logger used for logging debug and error
    #   messages to.

    def initialize(stream_name:, delay:, logger: Logger.new("/dev/null"))
      @stream_name = stream_name
      @delay = delay
      @logger = logger

      @consumer = Consumer.new(name: "delayer", stream_name: "#{stream_name}.delay", logger: logger)
      @batch = []
    end

    # Loops and blocks forever processing delay messages read from a delay
    # stream.

    def run
      loop { run_once }
    end

    # Reads and processes a single batch of delay messages from a delay
    # stream. You usually want to use the #run method instead, which
    # loops/blocks forever.

    def run_once
      @consumer.run_once do |messages|
        messages.each do |message|
          seconds_to_sleep = (message.message_id.to_f / 1_000) + @delay.to_f - Time.now.to_f

          if seconds_to_sleep > 0
            if @batch.size > 0
              id = @batch.last.message_id

              deliver

              @consumer.commit id
            end

            sleep(seconds_to_sleep + 1)
          end

          @batch << message
        end

        deliver
      end
    rescue StandardError => e
      @logger.error e

      sleep 5

      retry
    end

    private

    def deliver
      return if @batch.size.zero?

      @logger.debug "Delayed #{@batch.size} messages for #{@delay.to_f} seconds on stream #{@stream_name}"

      Redstream.connection_pool.with do |redis|
        redis.pipelined do |pipeline|
          @batch.each do |message|
            pipeline.xadd(Redstream.stream_key_name(@stream_name), { payload: message.fields["payload"] })
          end
        end

        redis.xdel(Redstream.stream_key_name("#{@stream_name}.delay"), @batch.map(&:message_id))
      end

      @batch = []
    end
  end
end
