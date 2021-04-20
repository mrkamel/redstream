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
    # method. The bulk method writes delay messages for the records to kafka,
    # then yields and the writes the message for immediate retrieval. The
    # method must ensure that the same set of records is used for the delay
    # messages and the instant messages. Thus, you optimally, pass an array of
    # records to it. If you pass an ActiveRecord::Relation, the method
    # converts it to an array, i.e. loading all matching records into memory.
    #
    # @param records [#to_a] The object/objects that will be updated or deleted

    def bulk(records)
      records_array = Array(records)

      message_ids = bulk_delay(records_array)

      yield

      bulk_queue(records_array)
      bulk_delete(records_array, message_ids)
    end

    # @api private
    #
    # Writes delay messages to a delay stream in redis.
    #
    # @param records [#to_a] The object/objects that will be updated or deleted
    #
    # @return The redis message ids

    def bulk_delay(records)
      res = records.each_slice(250).flat_map do |slice|
        Redstream.connection_pool.with do |redis|
          redis.pipelined do
            slice.each do |object|
              redis.xadd Redstream.stream_key_name("#{stream_name(object)}.delay"), payload: JSON.dump(object.redstream_payload)
            end
          end
        end
      end

      Redstream.connection_pool.with do |redis|
        redis.wait(@wait, 0) if @wait
      end

      res
    end

    # @api private
    #
    # Deletes delay message from a delay stream in redis.
    #
    # @param records [#to_a] The object/objects that have beeen updated or deleted
    # @param ids [#to_a] The ids of the respective delay messages

    def bulk_delete(records, ids)
      records.each_with_index.each_slice(250) do |slice|
        Redstream.connection_pool.with do |redis|
          redis.pipelined do
            slice.each do |object, index|
              redis.xdel Redstream.stream_key_name("#{stream_name(object)}.delay"), ids[index]
            end
          end
        end
      end
    end

    # @api private
    #
    # Writes messages to a stream in redis for immediate retrieval.
    #
    # @param records [#to_a] The object/objects that will be updated deleted

    def bulk_queue(records)
      records.each_slice(250) do |slice|
        Redstream.connection_pool.with do |redis|
          redis.pipelined do
            slice.each do |object|
              redis.xadd Redstream.stream_key_name(stream_name(object)), payload: JSON.dump(object.redstream_payload)
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
    # @param object The object that will be updated, deleted, etc.
    #
    # @return The redis message id

    def delay(object)
      Redstream.connection_pool.with do |redis|
        res = redis.xadd(Redstream.stream_key_name("#{stream_name(object)}.delay"), payload: JSON.dump(object.redstream_payload))
        redis.wait(@wait, 0) if @wait
        res
      end
    end

    # @api private
    #
    # Deletes a single delay message from a delay stream in redis.
    #
    # @param object The object that has been updated, deleted, ect.
    # @param id The redis message id

    def delete(object, id)
      Redstream.connection_pool.with do |redis|
        redis.xdel Redstream.stream_key_name("#{stream_name(object)}.delay"), id
      end
    end

    # @api private
    #
    # Writes a single message to a stream in redis for immediate retrieval.
    #
    # @param object The object hat will be updated, deleted, etc.

    def queue(object)
      Redstream.connection_pool.with do |redis|
        redis.xadd Redstream.stream_key_name(stream_name(object)), payload: JSON.dump(object.redstream_payload)
      end

      true
    end

    private

    def stream_name(object)
      synchronize do
        @stream_name_cache[object.class] ||= object.class.redstream_name
      end
    end
  end
end
