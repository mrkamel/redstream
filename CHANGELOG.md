# CHANGELOG

## v0.2.0
* Add commit safety check before yielding messages
* No more retry in `run_once`

## v0.1.1
* Fix missing queue message in `after_commit on: :destroy`

## v0.1.0
* No longer queue/delay in `after_save`/`after_commit` if no changes occurred
* Added `Redstream.stream_size`
