# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'clean_sweep/version'

Gem::Specification.new do |spec|
  spec.name          = "cleansweep"
  spec.version       = CleanSweep::VERSION
  spec.authors       = ["Bill Kayser"]
  spec.email         = ["bkayser@newrelic.com"]
  spec.summary       = %q{Utility to purge or archive rows in mysql tables}

  spec.platform      = Gem::Platform::RUBY
  spec.required_ruby_version = '~> 2'

  spec.description   = <<-EOF
     Purge data from mysql innodb tables efficiently with low overhead and impact.
     Based on the Percona pt-archive utility.
  EOF
  spec.homepage      = "http://bkayser.github.com/cleansweep"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^spec/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'activerecord', '>= 3.0'
  spec.add_runtime_dependency 'newrelic_rpm'
  spec.add_runtime_dependency 'mysql2', '~> 0.3'

  spec.add_development_dependency 'pry', '~> 0'
  spec.add_development_dependency 'timecop', '~> 0.7.1'
  spec.add_development_dependency 'bundler', '~> 1.7'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.1'
  spec.add_development_dependency 'factory_girl', '~> 4.4'
  spec.add_development_dependency 'awesome_print', '~>1.2'
end
