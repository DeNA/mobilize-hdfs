# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mobilize-hdfs/version'

Gem::Specification.new do |gem|
  gem.name          = "mobilize-hdfs"
  gem.version       = Mobilize::Hdfs::VERSION
  gem.authors       = ["Cassio Paes-Leme"]
  gem.email         = ["cpaesleme@dena.com"]
  gem.description   = %q{Adds hdfs read, write, and copy support to mobilize-ssh}
  gem.summary       = %q{Adds hdfs read, write, and copy support to mobilize-ssh}
  gem.homepage      = "http://github.com/dena/mobilize-hdfs"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_runtime_dependency "mobilize-ssh","1.29"
end
