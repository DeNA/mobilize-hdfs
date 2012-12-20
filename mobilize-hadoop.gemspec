# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mobilize-hadoop/version'

Gem::Specification.new do |gem|
  gem.name          = "mobilize-hadoop"
  gem.version       = Mobilize::Hadoop::VERSION
  gem.authors       = ["Cassio Paes-Leme"]
  gem.email         = ["cpaesleme@ngmoco.com"]
  gem.description   = %q{Adds hadoop, hive, and streaming support to mobilize-ssh}
  gem.summary       = %q{Adds hadoop, hive, and streaming support to mobilize-ssh}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_runtime_dependency "mobilize-ssh","1.0.3"
end
