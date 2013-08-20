source 'https://rubygems.org'

# Specify your gem's dependencies in mobilize-hadoop.gemspec
gemspec

group :test, :development do
  if File.exists? File.expand_path("../../mobilize-base", __FILE__)
    gem 'mobilize-base', path: File.expand_path("../../mobilize-base", __FILE__)
  end
  if File.exists? File.expand_path("../../mobilize-ssh", __FILE__)
    gem 'mobilize-ssh' , path: File.expand_path("../../mobilize-ssh", __FILE__)
  end
end
