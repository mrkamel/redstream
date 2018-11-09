
require "redis"
require "json"
require "thread"

require "redstream/version"
require "redstream/message"
require "redstream/consumer"
require "redstream/producer"
require "redstream/delayer"
require "redstream/model"
require "redstream/trimmer"

module Redstream
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

