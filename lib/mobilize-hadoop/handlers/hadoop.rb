module Mobilize
  module Hadoop
    def Hadoop.config
      Base.config('hadoop')
    end

    def Hadoop.exec_path
      Hadoop.config['hadoop']['exec_path']
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

    def Hadoop.gateway_node(cluster)
      Hadoop.clusters[cluster]['gateway_node']
    end

    def Hadoop.read_limit
      Hadoop.config['read_limit']
    end

    def Hadoop.root(cluster)
      namenode = Hadoop.clusters[cluster]['namenode']
      "hdfs://#{namenode['name']}:#{namenode['port']}/"
    end

    def Hadoop.run(command,file_hash={},cluster=Hadoop.output_cluster,su_user=nil)
      h_command = if command.starts_with?("hadoop")
                    command.sub("hadoop",Hadoop.exec_path)
                  else
                    "#{Hadoop.exec_path} #{command}"
                  end
      gateway_node = Hadoop.gateway_node(cluster)
      Ssh.run(gateway_node,h_command,file_hash,su_user)
    end

    def Hadoop.dfs(command,file_hash={},cluster=Hadoop.output_cluster,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      command = "dfs -fs #{Hadoop.root(cluster)} #{command}"
      Hadoop.run(command,file_hash,cluster,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hadoop.rm(path,cluster=Hadoop.output_cluster,su_user=nil)
      #ignore errors due to missing file
      Hadoop.dfs("rm #{path}",{},cluster,su_user)
    end

    def Hadoop.rmr(dir,cluster=Hadoop.output_cluster,su_user=nil)
      #ignore errors due to missing dir
      Hadoop.dfs("rmr #{dir}",{},cluster,su_user)
    end

    def Hadoop.job(command,file_hash={},cluster=Hadoop.output_cluster,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      Hadoop.run("job -fs #{Hadoop.root(cluster)} #{command}",file_hash,cluster,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hadoop.job_list(cluster=Hadoop.output_cluster)
      raw_list = Hadoop.job("list",{},cluster)
      raw_list.split("\n")[1..-1].join("\n").tsv_to_hash_array
    end

    def Hadoop.job_status(hadoop_job_id,cluster=Hadoop.output_cluster)
      raw_status = Hadoop.job("status #{hadoop_job_id}",{},cluster)
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

    def Hadoop.read(path,cluster=Hadoop.output_cluster,su_user=nil)
      gateway_node = Hadoop.gateway_node(cluster)
      #need to direct stderr to dev null since hadoop throws errors at being headed off
      command = "((#{Hadoop.exec_path} fs -cat #{path} | head -c #{Hadoop.read_limit}) > out.txt 2> /dev/null) && cat out.txt"
      Ssh.run(gateway_node,command,{},su_user)
    end

    def Hadoop.write(path,string,cluster=Hadoop.output_cluster,su_user=nil)
      file_hash = {'file.txt'=>string}
      Hadoop.rm(path) #remove old one
      write_command = "dfs -copyFromLocal file.txt #{path}"
      Hadoop.run(write_command,file_hash,cluster,su_user)
      return path
    end

    def Hadoop.copy(from_path,to_path,from_cluster=Hadoop.output_cluster,to_cluster=from_cluster,su_user=nil)
      Hadoop.rm(to_path,to_cluster) #remove to_path
      command = "dfs -cp #{from_url} #{to_url}"
      Hadoop.run(command,{},from_cluster,su_user)
      
    end

    def Hadoop.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      command = params['cmd']
      file_hash = Ssh.file_hash_by_stage_path(stage_path)
      su_user = s.params['su_user']
      cluster = s.params['cluster'] || Hadoop.output_cluster
      if su_user and !Ssh.sudoers(cluster).include?(u.name)
        raise "You do not have su permissions for this cluster"
      elsif su_user.nil? and Ssh.su_all_users(cluster)
        su_user = u.name
      end
      out_string = Hadoop.run(cluster,command,file_hash,su_user).to_s
      out_url = "hadoop://#{Hadoop.output_namenode}/#{Hadoop.output_dir}/hadoop/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string)
      out_url
    end

    def Hadoop.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      #target_path = params['target']
      su_user = params['su_user']
      if su_user and !Ssh.sudoers(Hadoop.node).include?(u.name)
        raise "You do not have su permissions for this node"
      elsif su_user.nil? and Ssh.su_all_users(node)
        su_user = u.name
      end
      source_dst = s.source_dst(source_path)
      if source_dst.handler=='hadoop'
        #do Hadoop.copy
        Hadoop.copy(from_path,to_path,from_namenode,to_namenode,su_user)
      else
        #read normally, do Hadoop.write
        Hadoop.write(to_path,source_dst.read,to_namenode,su_user)
      end
      out_url = "hadoop://#{Hadoop.output_namenode}/#{Hadoop.output_dir}/hadoop/#{stage_path}/out"
      Dataset.write_by_url(url,string)
      out_url
    end
  end
end
