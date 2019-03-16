# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'redstream/version'

Gem::Specification.new do |spec|
  spec.name          = "redstream"
  spec.version       = Redstream::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@plainpicture.de"]
  spec.summary       = %q{Using redis streams to keep your primary database in sync with secondary datastores}
  spec.description   = %q{Using redis streams to keep your primary database in sync with secondary datastores}
  spec.homepage      = "https://github.com/mrkamel/redstream"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "activerecord"
  spec.add_development_dependency "database_cleaner"
  spec.add_development_dependency "sqlite3", "1.3.13"
  spec.add_development_dependency "factory_bot"
  spec.add_development_dependency "timecop"
  spec.add_development_dependency "concurrent-ruby"
  spec.add_development_dependency "mocha"

  spec.add_dependency "connection_pool"
  spec.add_dependency "activesupport"
  spec.add_dependency "redis", ">= 4.1.0"
  spec.add_dependency "json"
end

