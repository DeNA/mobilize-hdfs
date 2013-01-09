module Mobilize
  module Hdfs
    def Hdfs.config
      Base.config('hdfs')
    end

    def Hdfs.hadoop_exec_path(cluster)
      Hdfs.config['clusters'][cluster]['hadoop_exec_path']
    end

    def Hdfs.clusters
      Hdfs.config['clusters']
    end

    def Hdfs.output_cluster
      Hdfs.config['output_cluster']
    end

    def Hdfs.output_dir
      Hdfs.config['output_dir']
    end

    def Hdfs.gateway_node(cluster)
      Hdfs.clusters[cluster]['gateway_node']
    end

    def Hdfs.read_limit
      Hdfs.config['read_limit']
    end

    def Hdfs.root(cluster)
      namenode = Hdfs.clusters[cluster]['namenode']
      "hdfs://#{namenode['name']}:#{namenode['port']}/"
    end

    def Hdfs.run(command,file_hash={},cluster=Hdfs.output_cluster,su_user=nil)
      h_command = if command.starts_with?("hadoop")
                    command.sub("hadoop",Hdfs.hadoop_exec_path(cluster))
                  else
                    "#{Hdfs.hadoop_exec_path(cluster)} #{command}"
                  end
      gateway_node = Hdfs.gateway_node(cluster)
      Ssh.run(gateway_node,h_command,file_hash,su_user)
    end

    def Hdfs.dfs(command,file_hash={},cluster=Hdfs.output_cluster,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      command = "dfs -fs #{Hdfs.root(cluster)} #{command}"
      Hdfs.run(command,file_hash,cluster,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hdfs.rm(path,cluster=Hdfs.output_cluster,su_user=nil)
      #ignore errors due to missing file
      Hdfs.dfs("rm #{path}",{},cluster,su_user)
    end

    def Hdfs.rmr(dir,cluster=Hdfs.output_cluster,su_user=nil)
      #ignore errors due to missing dir
      Hdfs.dfs("rmr #{dir}",{},cluster,su_user)
    end

    def Hdfs.job(command,file_hash={},cluster=Hdfs.output_cluster,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      Hdfs.run("job -fs #{Hdfs.root(cluster)} #{command}",file_hash,cluster,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hdfs.job_list(cluster=Hdfs.output_cluster)
      raw_list = Hdfs.job("list",{},cluster)
      raw_list.split("\n")[1..-1].join("\n").tsv_to_hash_array
    end

    def Hdfs.job_status(hdfs_job_id,cluster=Hdfs.output_cluster)
      raw_status = Hdfs.job("status #{hdfs_job_id}",{},cluster)
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

    def Hdfs.read(path,cluster=Hdfs.output_cluster,su_user=nil)
      gateway_node = Hdfs.gateway_node(cluster)
      #need to direct stderr to dev null since hdfs throws errors at being headed off
      command = "((#{Hdfs.hadoop_exec_path(cluster)} fs -cat #{path} | head -c #{Hdfs.read_limit}) > out.txt 2> /dev/null) && cat out.txt"
      Ssh.run(gateway_node,command,{},su_user)
    end

    def Hdfs.write(path,string,cluster=Hdfs.output_cluster,su_user=nil)
      file_hash = {'file.txt'=>string}
      Hdfs.rm(path) #remove old one
      write_command = "dfs -copyFromLocal file.txt #{path}"
      Hdfs.run(write_command,file_hash,cluster,su_user)
      return path
    end

    def Hdfs.copy(from_path,to_path,from_cluster=Hdfs.output_cluster,to_cluster=from_cluster,su_user=nil)
      Hdfs.rm(to_path,to_cluster) #remove to_path
      command = "dfs -cp #{from_url} #{to_url}"
      Hdfs.run(command,{},from_cluster,su_user)
      
    end

    def Hdfs.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      command = params['cmd']
      file_hash = Ssh.file_hash_by_stage_path(stage_path)
      su_user = s.params['su_user']
      cluster = s.params['cluster'] || Hdfs.output_cluster
      if su_user and !Ssh.sudoers(cluster).include?(u.name)
        raise "You do not have su permissions for this cluster"
      elsif su_user.nil? and Ssh.su_all_users(cluster)
        su_user = u.name
      end
      out_string = Hdfs.run(cluster,command,file_hash,su_user).to_s
      out_url = "hdfs://#{Hdfs.output_namenode}/#{Hdfs.output_dir}/hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string)
      out_url
    end

    def Hdfs.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      #target_path = params['target']
      su_user = params['su_user']
      if su_user and !Ssh.sudoers(Hdfs.node).include?(u.name)
        raise "You do not have su permissions for this node"
      elsif su_user.nil? and Ssh.su_all_users(node)
        su_user = u.name
      end
      source_dst = s.source_dst(source_path)
      if source_dst.handler=='hdfs'
        #do Hdfs.copy
        Hdfs.copy(from_path,to_path,from_namenode,to_namenode,su_user)
      else
        #read normally, do Hdfs.write
        Hdfs.write(to_path,source_dst.read,to_namenode,su_user)
      end
      out_url = "hdfs://#{Hdfs.output_namenode}/#{Hdfs.output_dir}/hdfs/#{stage_path}/out"
      Dataset.write_by_url(url,string)
      out_url
    end
  end
end
