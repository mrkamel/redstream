
require "active_support/inflector"
require "connection_pool"
require "redis"
require "json"
require "thread"
require "set"

require "redstream/version"
require "redstream/lock"
require "redstream/message"
require "redstream/consumer"
require "redstream/producer"
require "redstream/delayer"
require "redstream/model"
require "redstream/trimmer"

module Redstream
  # Redstream uses the connection_pool gem to pool redis connections.  In case
  # you have a distributed redis setup (sentinel/cluster) or the default pool
  # size doesn't match your requirements, then you must specify the connection
  # pool. A connection pool is neccessary, because redstream is using blocking
  # commands. Please note, redis connections are somewhat cheap, so you better
  # specify the pool size to be large enough instead of running into
  # bottlenecks.
  #
  # @example
  #   Redstream.connection_pool = ConnectionPool.new(size: 50) do
  #     Redis.new("...")
  #   end

  def self.connection_pool=(connection_pool)
    @connection_pool = connection_pool
  end

  # Returns the connection pool instance or sets and creates a new connection
  # pool in case no pool is yet created.
  #
  # @return [ConnectionPool] The connection pool

  def self.connection_pool
    @connection_pool ||= ConnectionPool.new(size: 20) { Redis.new }
  end

  # Returns the max id of the specified stream, i.e. the id of the
  # last/newest message added. Returns nil for empty streams.
  #
  # @param stream_name [String] The stream name
  # @return [String, nil] The id of a stream's newest messages, or nil

  def self.max_stream_id(stream_name)
    @connection_pool.with do |redis|
      message = redis.xrevrange(stream_key_name(stream_name), "+", "-", "COUNT", 1)[0]

      return unless message

      message[0]
    end
  end

  # Returns the max committed id, i.e. the consumer's offset, for the specified
  # consumer name.
  #
  # @param consumer_name [String] the consumer name
  # @return [String, nil] The max committed offset, or nil

  def self.max_consumer_id(consumer_name)
    @connection_pool.with do |redis|
      redis.get offset_key_name(consumer_name)
    end
  end

  # @api private
  #
  # Generates the low level redis stream key name.
  #
  # @param stream_name A high level stream name
  # @return [String] A low level redis stream key name

  def self.stream_key_name(stream_name)
    "redstream:stream:#{stream_name}"
  end

  # @api private
  #
  # Generates the redis key name used for storing a consumer's current offset,
  # i.e. the maximum id successfully processed.
  #
  # @param consumer_name A high level consumer name
  # @return [String] A redis key name for storing a stream's current offset

  def self.offset_key_name(consumer_name)
    "redstream:offset:#{consumer_name}"
  end

  # @api private
  #
  # Generates the redis key name used for locking.
  #
  # @param name A high level name for the lock
  # @return [String] A redis key name used for locking

  def self.lock_key_name(name)
    "redstream:lock:#{name}"
  end
end

