Gem::Specification.new do |s|
  s.name            = 'logstash-output-clickhouse'
  s.version         = '0.1.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "This output lets you `POST` messages as JSON in a batched fashion to ClickHouse HTTP endpoint"
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["kmajk"]
  s.email           = 'k.majk@getresponse.com'
  s.homepage        = "http://getresponse.com"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','*.gemspec','*.md','Gemfile','LICENSE' ]

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "logstash-mixin-http_client", ">= 6.0.0", "<= 7.0.0"
  s.add_runtime_dependency 'mini_cache', ">= 1.0.0", "< 2.0.0"

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'sinatra'
  s.add_development_dependency 'webrick'
end
