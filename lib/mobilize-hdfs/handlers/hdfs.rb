module Mobilize
  module Hdfs
    def Hdfs.root(cluster=Hadoop.output_cluster)
      namenode = Hadoop.clusters[cluster]['namenode']
      "hdfs://#{namenode['name']}:#{namenode['port']}"
    end

    def Hdfs.run(command,cluster=Hadoop.output_cluster,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      command = "dfs -fs #{Hdfs.root(cluster)}/ #{command}"
      Hadoop.run(command,{},cluster,su_user)
    end

    def Hdfs.rm(path,cluster=Hadoop.output_cluster,su_user=nil)
      #ignore errors due to missing file
      Hdfs.run("rm #{path}",{},cluster,su_user)
    end

    def Hdfs.rmr(dir,cluster=Hadoop.output_cluster,su_user=nil)
      #ignore errors due to missing dir
      Hdfs.run("rmr #{dir}",{},cluster,su_user)
    end

    def Hdfs.read(path,cluster=Hadoop.output_cluster,su_user=nil)
      gateway_node = Hadoop.gateway_node(cluster)
      #need to direct stderr to dev null since hdfs throws errors at being headed off
      command = "((#{Hadoop.exec_path(cluster)} fs -cat #{path} | head -c #{Hadoop.read_limit}) > out.txt 2> /dev/null) && cat out.txt"
      Ssh.run(gateway_node,command,{},su_user)
    end

    def Hdfs.write(path,string,cluster=Hadoop.output_cluster,su_user=nil)
      file_hash = {'file.txt'=>string}
      Hdfs.rm(path) #remove old one
      write_command = "dfs -copyFromLocal file.txt #{path}"
      Hadoop.run(write_command,file_hash,cluster,su_user)
      return path
    end

    def Hdfs.copy(from_path,to_path,from_cluster=Hadoop.output_cluster,to_cluster=from_cluster,su_user=nil)
      Hdfs.rm(to_path,to_cluster) #remove to_path
      from_url = "#{Hadoop.root(from_cluster)}#{from_path}"
      to_url = "#{Hadoop.root(to_cluster)}#{to_path}"
      command = "dfs -cp #{from_url} #{to_url}"
      Hdfs.run(command,{},from_cluster,su_user)
      return to_url
    end

    def Hdfs.read_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      su_user = params['su_user']
      cluster = params['cluster'] || Hadoop.output_cluster
      source = params['source']
      if su_user and !Ssh.sudoers(Hadoop.gateway_node(cluster)).include?(u.name)
        raise "You do not have su permissions for this cluster"
      elsif su_user.nil? and Ssh.su_all_users(cluster)
        su_user = u.name
      end
      out_string = Hdfs.read(source,cluster,su_user).to_s
      out_url = "hdfs://#{cluster}/#{Hadoop.output_dir}/hdfs/#{stage_path}/out"
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
      if su_user and !Ssh.sudoers(Hadoop.gateway_node(cluster)).include?(u.name)
        raise "You do not have su permissions for this node"
      elsif su_user.nil? and Ssh.su_all_users(node)
        su_user = u.name
      end
      source_dst = s.source_dst(source_path)
      Hdfs.write(to_path,source_dst.read,to_namenode,su_user)
      out_url = "hdfs://#{Hadoop.output_namenode}/#{Hadoop.output_dir}/hdfs/#{stage_path}/out"
      Dataset.write_by_url(url,string)
      out_url
    end

    def Hdfs.copy_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      su_user = params['su_user']
      source_cluster = params['source_cluster'] || Hadoop.output_cluster
      target_cluster = params['target_cluster'] || Hadoop.output_cluster
      source = params['source']
      target = params['target']
      if su_user and !Ssh.sudoers(Hadoop.gateway_node(cluster)).include?(u.name)
        raise "You do not have su permissions for this cluster"
      elsif su_user.nil? and Ssh.su_all_users(cluster)
        su_user = u.name
      end
      out_string = Hdfs.copy(source,target,source_cluster,target_cluster,su_user).to_s
      out_url = "hdfs://#{cluster}/#{Hadoop.output_dir}/hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string)
      out_url
    end

    def Hdfs.read_by_dataset_path(dst_path)
      cluster, path = dst_path.split("/").ie{|p| [p.first,p[1..-1].join("/")]}
      Hdfs.read(path,cluster)
    end

    def Hdfs.write_by_dataset_path(dst_path,string)
      cluster, path = dst_path.split("/").ie{|p| [p.first,p[1..-1].join("/")]}
      Hdfs.write(path,string,cluster)
    end
  end
end
