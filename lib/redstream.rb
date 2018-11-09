
require "redis"
require "json"
require "thread"

require "redstream/version"
require "redstream/consumer"
require "redstream/producer"
require "redstream/delayer"
require "redstream/model"
require "redstream/trimmer"

module Redstream
  def self.stream_key_name(stream_name)
    "redstream:stream:#{stream_name}"
  end

  def self.offset_key_name(stream_name)
    "redstream:offset:#{stream_name}"
  end

  def self.lock_key_name(stream_name)
    "redstream:lock:#{stream_name}"
  end
end

