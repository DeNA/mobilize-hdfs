require 'rubygems'
require 'bundler/setup'
require 'minitest/autorun'
require 'redis/namespace'

$dir = File.dirname(File.expand_path(__FILE__))
#set test environment
ENV['MOBILIZE_ENV'] = 'test'
require 'mobilize-hdfs'
$TESTING = true
require "#{Mobilize::Ssh.home_dir}/test/test_helper"
