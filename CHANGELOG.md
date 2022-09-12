# CHANGELOG

## v0.4.4
* Remove deletion of delay messages after queues messages are sent from `.bulk`

## v0.4.3
* Change gemspec to support older redis versions again

## v0.4.2
* Fix pipelining with redis-rb 4.6.0

## v0.4.1
* Fix keyword argument usage of redis 4.5.1 in ruby 3

## v0.4.0
* Make delay message id params in queue methods optional

## v0.3.0
* Pipeline deletion of delay messages

## v0.2.0
* Delete delay messages after queue messages are sent

## v0.1.1
* Fix missing queue message in `after_commit on: :destroy`

## v0.1.0
* No longer queue/delay in `after_save`/`after_commit` if no changes occurred
* Added `Redstream.stream_size`
