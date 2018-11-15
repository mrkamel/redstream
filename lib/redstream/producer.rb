
module Redstream
  # A Redstream::Producer is responsible for writing the actual messages to
  # redis. This includes the delay messages as well as the messages for
  # immediate retrieval. Usually, you don't have to use a producer directly.
  # Instead, Redstream::Model handles all producer related interaction.
  # However, Redstream::Model is not able to recognize model updates resulting
  # from model updates via e.g. #update_all, #delete_all, etc, i.e. updates
  # which by-pass model callbacks. Thus, calls to e.g. #update_all must be
  # wrapped with `find_in_batches` and Redstream::Producer#bulk (see example),
  # to write these updates to the redis streams as well.
  #
  # @example
  #   producer = Redstream::Producer.new
  #
  #   User.where(confirmed: true).find_in_batches do |users|
  #     producer.bulk users do
  #       User.where(id: users.map(&:id)).update_all(send_mailing: true)
  #     end
  #   end

  class Producer
    include MonitorMixin

    # Initializes a new producer. In case you're using a distributed redis
    # setup, you can use redis WAIT to improve real world data safety via the
    # wait param.
    #
    # @param wait [Boolean, Integer] Defaults to false. Specify an integer to
    #   enable using redis WAIT for writing delay messages. Check out the
    #   redis docs for more info regarding WAIT.

    def initialize(wait: false)
      @wait = wait
      @stream_name_cache = {}

      super()
    end

    # Use to wrap calls to #update_all, #delete_all, etc. I.e. methods, which
    # by-pass model lifecycle callbacks (after_save, etc.), as Redstream::Model
    # can't recognize these updates and write them to redis streams
    # automatically. You need to pass the records to be updated to the bulk
    # method. This can be a single object, an array of objects, an
    # ActiveRecord::Relation or anything responding to #each or #find_each. The
    # bulk method that writes batches of delay messages to redis for these
    # records, yields and then writes the same messages again, but for
    # immediate retrieval.
    #
    # @param scope [#each, #find_each] The object/objects that will be updated,
    #   deleted, etc.

    def bulk(scope)
      bulk_delay(scope)

      yield

      bulk_queue(scope)
    end

    # @api private
    #
    # Writes delay messages to a delay stream in redis.
    #
    # @param scope [#each, #find_each] The object/objects that will be updated,
    #   deleted, etc.

    def bulk_delay(scope)
      enumerable(scope).each_slice(250) do |slice|
        Redstream.connection_pool.with do |redis|
          redis.pipelined do
            slice.map do |object|
              redis.xadd Redstream.stream_key_name("#{stream_name(object)}-delay"), "*", "payload", JSON.dump(object.redstream_payload)
            end
          end
        end
      end

      Redstream.connection_pool.with do |redis|
        redis.wait(@wait, 0) if @wait
      end

      true
    end

    # @api private
    #
    # Writes messages to a stream in redis for immediate retrieval.
    #
    # @param scope [#each, #find_each] The object/objects that will be updated,
    #   deleted, etc.

    def bulk_queue(scope)
      enumerable(scope).each_slice(250) do |slice|
        Redstream.connection_pool.with do |redis|
          redis.pipelined do
            slice.each do |object|
              redis.xadd Redstream.stream_key_name(stream_name(object)), "*", "payload", JSON.dump(object.redstream_payload)
            end
          end
        end
      end

      true
    end

    # @api private
    #
    # Writes a single delay message to a delay stream in redis.
    #
    # @param object The object hat will be updated, deleted, etc.

    def delay(object)
      Redstream.connection_pool.with do |redis|
        redis.xadd Redstream.stream_key_name("#{stream_name(object)}-delay"), "*", "payload", JSON.dump(object.redstream_payload)
        redis.wait(@wait, 0) if @wait
      end

      true
    end

    # @api private
    #
    # Writes a single message to a stream in redis for immediate retrieval.
    #
    # @param object The object hat will be updated, deleted, etc.

    def queue(object)
      Redstream.connection_pool.with do |redis|
        redis.xadd Redstream.stream_key_name(stream_name(object)), "*", "payload", JSON.dump(object.redstream_payload)
      end

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

