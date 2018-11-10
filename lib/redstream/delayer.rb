
module Redstream
  class Delayer
    def initialize(redis: Redis.new, stream_name:, delay:, logger: Logger.new("/dev/null"))
      @redis = redis
      @stream_name = stream_name
      @delay = delay
      @logger = logger

      @consumer = Consumer.new(redis: redis.dup, name: "#{stream_name}-delayer", stream_name: "#{stream_name}-delay", logger: logger)
      @batch = []
    end

    def run
      loop { run_once }
    end

    def run_once
      @consumer.run_once do |messages|
        messages.each do |message|
          seconds_to_sleep = message.id.to_f / 1_000 + @delay.to_f - Time.now.to_f

          if seconds_to_sleep > 0
            if @batch.size > 0
              id = @batch.last.id

              deliver

              @consumer.commit id
            end

            sleep(seconds_to_sleep + 1)
          end

          @batch << message
        end

        deliver
      end
    end

    private

    def deliver
      return if @batch.size.zero?

      @logger.debug "Delayed #{@batch.size} messages for #{@delay} seconds"

      @redis.pipelined do
        @batch.each do |message|
          @redis.xadd Redstream.stream_key_name(@stream_name), "*", "payload", message["payload"]
        end
      end

      @redis.xdel Redstream.stream_key_name("#{@stream_name}-delay"), @batch.map(&:id)

      @batch = []
    end
  end
end

