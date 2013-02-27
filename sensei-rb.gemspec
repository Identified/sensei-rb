$:.unshift File.expand_path("../lib", __FILE__)
require "sensei/version"

Gem::Specification.new do |s|
  s.name        = 'sensei-rb'
  s.version     = '0.1.0'
  s.date        = '2012-04-20'
  s.summary     = "Ruby client for SenseiDB"
  s.description = "A ruby client for SenseiDB."
  s.authors     = ["Jason Feng"]
  s.email       = 'jason@identified.com'
  s.homepage = 'https://github.com/Identified/sensei-rb'
  s.license  = 'MIT'

  s.add_development_dependency('bundler')
  s.add_development_dependency('rspec', '~> 2.13.0')

  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {spec,features}/*`.split("\n")
  s.require_paths = ["lib"]
end
