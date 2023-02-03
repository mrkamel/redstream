lib = File.expand_path("lib", __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "redstream/version"

Gem::Specification.new do |spec|
  spec.name          = "redstream"
  spec.version       = Redstream::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@plainpicture.de"]
  spec.summary       = "Using redis streams to keep your primary database in sync with secondary datastores"
  spec.description   = "Using redis streams to keep your primary database in sync with secondary datastores"
  spec.homepage      = "https://github.com/mrkamel/redstream"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "activesupport"
  spec.add_dependency "connection_pool"
  spec.add_dependency "json"
  spec.add_dependency "redis", ">= 4.1.0"
end
