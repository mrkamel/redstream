
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
  # Generates the redis key name used for storing a stream's current offset,
  # i.e. the maximum id successfully processed.
  #
  # @param stream_name A high level stream name
  # @return [String] A redis key name for storing a stream's current offset

  def self.offset_key_name(stream_name)
    "redstream:offset:#{stream_name}"
  end

  # @api private
  #
  # Generates the redis key name used for locking with regards to the
  # processing of a stream.
  #
  # @param stream_name A high level stream name
  # @return [String] A redis key name used for locking

  def self.lock_key_name(stream_name)
    "redstream:lock:#{stream_name}"
  end
end

