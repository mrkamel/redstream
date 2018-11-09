
# Redstream

Using redis streams to keep your primary database in sync with secondary
datastores (e.g. elasticsearch).

## Installation

First, install redis. Then, add this line to your application's Gemfile:

```ruby
gem 'redstream'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install redstream

## Usage

Include `Redstream::Model` in your model and add a call to
`readstream_callbacks`.

```ruby
class MyModel < ActiveRecord::Base
  include Redstream::Model

  # ...

  redstream_callbacks

  # ...
end
```

In a background process, you need to run a `Redstream::Consumer`, `Redstream::Delayer`
and a `Redstream::Trimmer`:

```ruby
Redstream::Consumer.new(stream_name: "products", value: `hostname`).run do |messages|
  # Update seconday datastore
end

# ...

Redstream::Delayer.new(stream_name: "products", value: `hostname`, delay: 5.minutes).run

# ...

RedStream::Trimmer.new(stream_name: "products", expiry: 1.day).run
```

`redstream_callbacks` adds `after_save`, `after_touch`, `after_destroy` and,
most importantly, `after_commit` callbacks which write messages, containing the
record id, to a redis stream. A background worker can then fetch those messages
and update secondary datastores.

More concretely, `after_save`, `after_touch` and `after_destroy` only write
"delay" messages to a special purpose redis stream. Only `after_commit` writes
messages to a redis stream for updating secondary datastores immediately. The
reasoning is simple: usually, i.e. by using only one way to update secondary
datastores, namely `after_save` or `after_commit`, any errors occurring in
between `after_save` and `after_commit` result in inconsistencies between your
primary and secondary datastore. By using these kinds of "delay" messages
triggered by `after_save` and fetched after e.g. 5 minutes, errors occurring in
between `after_save` and `after_commit` can be fixed when the delay message get
processed.

Any messages are fetched in batches, such that elasticsearch can be updated
using its bulk API.

