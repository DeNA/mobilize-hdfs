require "mobilize-hdfs/version"
require "mobilize-ssh"

module Mobilize
  module Hdfs
    def Hdfs.home_dir
      File.expand_path('..',File.dirname(__FILE__))
    end
  end
end
require "mobilize-hdfs/handlers/hadoop"
require "mobilize-hdfs/handlers/hdfs"
