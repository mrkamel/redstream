
# Redstream

**Using redis streams to keep your primary database in sync with secondary
datastores (e.g. elasticsearch).**

[![Build Status](https://secure.travis-ci.org/mrkamel/redstream.png?branch=master)](http://travis-ci.org/mrkamel/redstream)

## Installation

First, install redis. Then, add this line to your application's Gemfile:

```ruby
gem 'redstream'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install redstream

## Reference Docs

The reference docs can be found at
[https://www.rubydoc.info/github/mrkamel/redstream/master](https://www.rubydoc.info/github/mrkamel/redstream/master).

## Usage

Include `Redstream::Model` in your model and add a call to
`redstream_callbacks`.

```ruby
class MyModel < ActiveRecord::Base
  include Redstream::Model

  # ...

  redstream_callbacks

  # ...
end
```

`redstream_callbacks` adds `after_save`, `after_touch`, `after_destroy` and,
most importantly, `after_commit` callbacks which write messages, containing the
record id, to a redis stream. A background worker can then fetch those messages
and update secondary datastores.

In a background process, you need to run a `Redstream::Consumer`, `Redstream::Delayer`
and a `Redstream::Trimmer`:

```ruby
Redstream::Consumer.new(stream_name: Product.redstream_name, name: "consumer").run do |messages|
  # Update seconday datastore
end

# ...

Redstream::Delayer.new(stream_name: Product.redstream_name, delay: 5.minutes).run

# ...

RedStream::Trimmer.new(stream_name: Product.redstream_name, expiry: 1.day).run
```

As all of them are blocking, you should run them in individual threads. But as
none of them must be stopped gracefully, this can be as simple as:

```ruby
Thread.new do
  Redstream::Consumer.new("...").run do |messages|
    # ...
  end
end
```

More concretely, `after_save`, `after_touch` and `after_destroy` only write
"delay" messages to an additional redis stream. Delay message are like any
other messages, but they get processed by a `Redstream::Delayer` and the
`Delayer`will wait for some (configurable) delay/time before processing them.
As the `Delayer` is neccessary to fix inconsistencies, the delay must be at
least as long as your maxiumum database transaction time. Contrary,
`after_commit` writes messages to a redis stream from which the messages can
be fetched immediately to keep the secondary datastores updated in
near-realtime. The reasoning of all this is simple: usually, i.e. by using only
one way to update secondary datastores, namely `after_save` or `after_commit`,
any errors occurring in between `after_save` and `after_commit` result in
inconsistencies between your primary and secondary datastore. By using these
kinds of "delay" messages triggered by `after_save` and fetched after e.g. 5
minutes, errors occurring in between `after_save` and `after_commit` can be
fixed when the delay message get processed.

Any messages are fetched in batches, such that e.g. elasticsearch can be
updated using its bulk API. For instance, depending on which elasticsearch ruby
client you are using, the reindexing code regarding elasticsearch will look
similar to:

```ruby
Thread.new do
  Redstream::Consumer.new(stream_name: Product.redstream_name, name: "indexer").run do |messages|
    ids = messages.map { |message| message.payload["id"] }

    ProductIndex.import Product.where(id: ids)
  end
end

Thread.new { Redstream::Delayer.new(stream_name: Product.redstream_name, delay: 5.minutes).run }
Thread.new { RedStream::Trimmer.new(stream_name: Product.redstream_name, expiry: 1.day).run }
```

You should run a consumer per `(stream_name, name)` tuple on multiple hosts for
high availability. They'll use a redis based locking mechanism to ensure that
only one consumer is consuming messages per tuple while the others are
hot-standbys, i.e. they'll take over in case the currently active instance
dies. The same stands for delayers and trimmers.

Please note: if you have multiple kinds of consumers for a single model/topic,
then you must use distinct names. Assume you have an indexer, which updates a
search index for a model and a cacher, which updates a cache store for a model:

```ruby
Redstream::Consumer.new(stream_name: Product.redstream_name, name: "indexer").run do |messages|
  # ...
end

Redstream::Consumer.new(stream_name: Product.redstream_name, name: "cacher").run do |messages|
  # ...
end
```

# Consumer, Delayer, Trimmer, Producer

A `Consumer` fetches messages that have been added to a redis stream via
`after_commit` or by a `Delayer`, i.e. messages that are available for
immediate retrieval/reindexing/syncing.

A `Delayer` fetches messages that have been added to a second redis stream via
`after_save`, `after_touch` and `after_destroy` to be retrieved after a certain
configurable amount of time (5 minutes usually) to fix inconsistencies. The
amount of time must be longer than your maximum database transaction time at
least.

A `Trimmer` is responsible to finally remove messages from redis streams.
Without the `Trimmer` messages will fill up your redis server and redis will
finally crash due to out of memory errors. However, a `Delayer` deletes
messages from a delay stream, after it moved/copied them, so why not simply
delete the messages after they have been consumed and successfully processed by
a `Consumer`? Well, this will probably be added in the future, but please note
that you should have only one `Delayer` and only one `Trimmer` per stream.
Contrary, you can have as many `Consumer` instances per stream as you like.
Actually, if you need to replicate updates into more than one secondary
datastore, you will have to use multiple `Consumer` instances per stream.
Moreover, to name another use case, elasticsearch requires to denormalize data
and you can e.g. use a second `Consumer` to cascade updates to dependent
indices stroring denormalized data.

A `Producer` adds messages to the concrete redis streams, and you
can actually pass a concrete `Producer` instance via `redstream_callbacks`:

```ruby
class Product < ActiveRecord::Base
  include Redstream::Model

  # ...

  redstream_callbacks producer: Redstream::Producer.new("...")

  # ...
end
```

As you might recognize, `Redstream::Model` is of course only able to send
messages to redis streams for model lifecyle callbacks. This is however not
the case for `#update_all`:

```ruby
Product.where(on_stock: true).update_all(featured: true)
```

To capture those updates as well, you need to change:

```ruby
Product.where(on_stock: true).update_all(featured: true)
```

to

```ruby
Product.where(on_stock: true).find_in_batches do |products|
  producer.bulk products do
    Product.where(id: products.map(&:id)).update_all(featured: true)
  end
end
```

The `Producer` will write a message for every matched record into the delay
stream before `update_all` is called and will write another message for every
record to the main stream after `update_all` is called - just like it is done
within the model lifecycle callbacks. But why do you need to do it in batches?
Imagine the following:

```ruby
Product.where(featured: true).where("price > 20").update_all(featured: false)
```

Now we naively pass the `ActiveRecord::Relation` to `Redstream::Producer#bulk`:

```ruby
  products = Product.where(featured: true).where("price > 20")

  producer.bulk products do
    products.update_all(featured: false)
  end
```

Here, the matching records passed written before `update_all` (delay messages)
and after `update_all` (instant messages) will differ, resulting in
inconsistencies. This would be fixed when the delay messages get processed, but
still, the situation is undesirable and can be mitigated by looping over the
batches via `find_in_batches` like shown above.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mrkamel/redstream

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

