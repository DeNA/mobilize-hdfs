module Mobilize
  module Hdfs
    def Hdfs.root(cluster)
      namenode = Hadoop.clusters[cluster]['namenode']
      "hdfs://#{namenode['name']}:#{namenode['port']}"
    end

    def Hdfs.run(command,cluster,user)
      command = ["-",command].join unless command.starts_with?("-")
      command = "dfs -fs #{Hdfs.root(cluster)}/ #{command}"
      Hadoop.run(command,cluster,user)
    end

    def Hdfs.rm(target_path,user)
      #ignore errors due to missing file
      cluster,cluster_path = Hdfs.resolve_path(target_path)
      begin
        Hdfs.run("rm '#{cluster_path}'",cluster,user)
        return true
      rescue
        return false
      end
    end

    def Hdfs.rmr(target_dir,user)
      #ignore errors due to missing dir
      cluster,cluster_dir = Hdfs.resolve_path(target_dir)
      begin
        Hdfs.run("rmr '#{cluster_dir}'",cluster,user)
        return true
      rescue
        return false
      end
    end

    def Hdfs.read(path,user)
      cluster, cluster_path = Hdfs.resolve_path(path)
      gateway_node = Hadoop.gateway_node(cluster)
      #need to direct stderr to dev null since hdfs throws errors at being headed off
      command = "((#{Hadoop.exec_path(cluster)} fs -fs '#{Hdfs.namenode_path(path)}' -cat #{cluster_path}"
      command += " | head -c #{Hadoop.read_limit}) > out.txt 2> /dev/null) && cat out.txt"
      response = Ssh.run(gateway_node,command,user)
      if response.length==Hadoop.read_limit
        raise "Hadoop read limit reached -- please reduce query size"
      end
      response
    end

    def Hdfs.resolve_path(path)
      if path.starts_with?("/")
        return [Hadoop.output_cluster,path]
      #determine if first term in path is a cluster name
      elsif Hadoop.clusters.keys.include?(path.split("/").first)
        return path.split("/").ie{|p| [p.first,"/#{p[1..-1].join("/")}"]}
      else
        return [nil,nil]
      end
    end

    def Hdfs.namenode_path(path)
      cluster, cluster_path = Hdfs.resolve_path(path)
      "#{Hdfs.root(cluster)}#{cluster_path}"
    end

    def Hdfs.write(path,string,user)
      file_hash = {'file.txt'=>string}
      cluster = Hdfs.resolve_path(path).first
      Hdfs.rm(path,user) #remove old one if any
      write_command = "dfs -copyFromLocal file.txt '#{Hdfs.namenode_path(path)}'"
      Hadoop.run(write_command,cluster,user,file_hash)
      return Hdfs.namenode_path(path)
    end

    def Hdfs.copy(source_path,target_path,user)
      Hdfs.rm(target_path,user) #remove to_path
      source_cluster = Hdfs.resolve_path(source_path).first
      command = "dfs -cp '#{Hdfs.namenode_path(source_path)}' '#{Hdfs.namenode_path(target_path)}'"
      #copy operation implies access to target_url from source_cluster
      Hadoop.run(command,source_cluster,user)
      return Hdfs.namenode_path(target_path)
    end

    def Hdfs.read_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hdfs.resolve_path(source_path)
      raise "unable to resolve source path" if source_cluster.nil?

      node = Hadoop.gateway_node(source_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      source_path = "#{source_cluster}#{source_cluster_path}"
      out_string = Hdfs.read(source_path,user).to_s
      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hdfs.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      target_path = params['target']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hdfs.resolve_path(source_path)
      if source_cluster.nil?
        #not hdfs
        gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
        #return blank response if there are no slots available
        return nil unless gdrive_slot
        source_dst = s.source_dsts(gdrive_slot).first
        Gdrive.unslot_worker_by_path(stage_path)
      else
        source_path = "#{source_cluster}#{source_cluster_path}"
        source_dst = Dataset.find_or_create_by_handler_and_path("hdfs",source_path)
      end

      #determine cluster for target
      target_cluster, target_cluster_path = Hdfs.resolve_path(target_path)
      raise "unable to resolve target path" if target_cluster.nil?

      node = Hadoop.gateway_node(target_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      target_path = "#{target_cluster}#{target_cluster_path}"
      in_string = source_dst.read(user)
      out_string = Hdfs.write(target_path,in_string,user)

      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hdfs.copy_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      source_path = params['source']
      target_path = params['target']
      user = params['user']
      #check for source in hdfs format
      source_cluster, source_cluster_path = Hdfs.resolve_path(source_path)
      raise "unable to resolve source path" if source_cluster.nil?

      #determine cluster for target
      target_cluster, target_cluster_path = Hdfs.resolve_path(target_path)
      raise "unable to resolve target path" if target_cluster.nil?

      node = Hadoop.gateway_node(source_cluster)
      if user and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for #{node}"
      elsif user.nil? and Ssh.su_all_users(node)
        user = u.name
      end

      source_path = "#{source_cluster}#{source_cluster_path}"
      target_path = "#{target_cluster}#{target_cluster_path}"
      out_string = Hdfs.copy(source_path,target_path,user)

      out_url = "hdfs://#{Hadoop.output_cluster}#{Hadoop.output_dir}hdfs/#{stage_path}/out"
      Dataset.write_by_url(out_url,out_string,Gdrive.owner_name)
      out_url
    end

    def Hdfs.read_by_dataset_path(dst_path,user)
      Hdfs.read(dst_path,user)
    end

    def Hdfs.write_by_dataset_path(dst_path,string,user)
      Hdfs.write(dst_path,string,user)
    end
  end
end
