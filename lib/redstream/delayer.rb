
module Redstream
  class Delayer
    def initialize(redis: Redis.new, stream_name:, delay:, value:, logger: Logger.new("/dev/null"))
      @redis = redis
      @stream_name = stream_name
      @delay = delay
      @logger = logger

      @consumer = Consumer.new(redis: redis.dup, stream_name: "#{stream_name}-delay", value: value, logger: logger)
      @batch = []
    end

    def run
      loop { run_once }
    end

    def run_once
      @consumer.run_once do |messages|
        messages.each do |message|
          diff = (message.id.to_f / 1_000) + @delay - Time.now.utc.to_f

          if diff > 0
            if @batch.size > 0
              deliver

              @consumer.commit @batch.last.id
            end

            sleep(diff + 5)
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

      @batch = []
    end
  end
end

