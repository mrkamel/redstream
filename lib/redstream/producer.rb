
module Redstream
  class Producer
    include MonitorMixin

    def initialize(redis: Redis.new)
      @redis = redis
      @stream_name_cache = {}

      super()
    end

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    def bulk_delay(scope)
      enumerable(scope).each_slice(250) do |slice|
        @redis.pipelined do
          slice.map do |object|
            @redis.xadd Redstream.stream_key_name("#{stream_name(object)}-delay"), "*", "payload", JSON.dump(object.redstream_payload)
          end
        end
      end

      true
    end

    def bulk_queue(scope)
      enumerable(scope).each_slice(250) do |slice|
        @redis.pipelined do
          slice.each do |object|
            @redis.xadd Redstream.stream_key_name(stream_name(object)), "*", "payload", JSON.dump(object.redstream_payload)
          end
        end
      end

      true
    end

    def delay(object)
      @redis.xadd Redstream.stream_key_name("#{stream_name(object)}-delay"), "*", "payload", JSON.dump(object.redstream_payload)

      true
    end

    def queue(object)
      @redis.xadd Redstream.stream_key_name(stream_name(object)), "*", "payload", JSON.dump(object.redstream_payload)

      true
    end

    private

    def stream_name(object)
      synchronize do
        @stream_name_cache[object.class] ||= object.class.name.pluralize.underscore.gsub("/", "_")
      end
    end

    def enumerable(scope)
      return scope.find_each if scope.respond_to?(:find_each)
      return scope if scope.respond_to?(:each)

      Array(scope)
    end
  end
end

