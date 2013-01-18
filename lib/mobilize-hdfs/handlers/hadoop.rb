module Mobilize
  module Hadoop
    def Hadoop.config
      Base.config('hadoop')
    end

    def Hadoop.exec_path(cluster)
      Hadoop.config['clusters'][cluster]['exec_path']
    end

    def Hadoop.gateway_node(cluster)
      Hadoop.clusters[cluster]['gateway_node']
    end

    def Hadoop.clusters
      Hadoop.config['clusters']
    end

    def Hadoop.output_cluster
      Hadoop.config['output_cluster']
    end

    def Hadoop.output_dir
      Hadoop.config['output_dir']
    end

    def Hadoop.read_limit
      Hadoop.config['read_limit']
    end

    def Hadoop.job(command,cluster,user,file_hash={})
      command = ["-",command].join unless command.starts_with?("-")
      Hadoop.run("job -fs #{Hdfs.root(cluster)} #{command}",cluster,user,file_hash).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hadoop.job_list(cluster)
      raw_list = Hadoop.job("list",{},cluster)
      raw_list.split("\n")[1..-1].join("\n").tsv_to_hash_array
    end

    def Hadoop.job_status(hdfs_job_id,cluster)
      raw_status = Hadoop.job("status #{hdfs_job_id}",{},cluster)
      dhash_status = raw_status.strip.split("\n").map do |sline|
                       delim_index = [sline.index("="),sline.index(":")].compact.min
                       if delim_index
                         key,value = [sline[0..delim_index-1],sline[(delim_index+1)..-1]]
                         {key.strip => value.strip}
                       end
                     end.compact
      hash_status = {}
      dhash_status.each{|h| hash_status.merge!(h)}
      hash_status
    end

    def Hadoop.run(command,cluster,user,file_hash={})
      h_command = if command.starts_with?("hadoop")
                    command.sub("hadoop",Hadoop.exec_path(cluster))
                  else
                    "#{Hadoop.exec_path(cluster)} #{command}"
                  end
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,h_command,user,file_hash)
    end
  end
end
