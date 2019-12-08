require File.expand_path("../lib/redstream", __dir__)
require "active_record"
require "factory_bot"
require "database_cleaner"
require "timecop"
require "concurrent"
require "mocha"
require "rspec/instafail"

RSpec.configure do |config|
  config.mock_with :mocha
end

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "/tmp/redstream.sqlite3")

ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS products"

ActiveRecord::Base.connection.create_table :products do |t|
  t.string :title
  t.timestamps
end

class Product < ActiveRecord::Base
  include Redstream::Model

  redstream_callbacks

  def redstream_payload
    { id: id }
  end
end

FactoryBot.define do
  factory :product do
    title { "title" }
  end
end

module SpecHelper
  def redis
    @redis ||= Redis.new
  end
end

RSpec.configure do |config|
  config.add_formatter(RSpec::Instafail)
  config.add_formatter(:progress)

  config.include SpecHelper
  config.include FactoryBot::Syntax::Methods

  config.before(:suite) do
    DatabaseCleaner.strategy = :truncation
  end

  config.around do |example|
    DatabaseCleaner.cleaning { example.run }
  end

  config.after do
    Redis.new.flushdb
  end
end
