module Mobilize
  module Hdfs
    #returns the hdfs path to the root of the cluster
    def Hdfs.root(cluster)
      namenode = Hadoop.config['clusters'][cluster]['namenode']
      "hdfs://#{namenode['name']}:#{namenode['port']}"
    end

    #replaces the cluster alias with a proper namenode path
    def Hdfs.hdfs_url(url)
      cluster = url.split("hdfs://").last.split("/").first
      #replace first instance
      url.sub("hdfs://#{cluster}",Hdfs.root(cluster))
    end

    def Hdfs.run(cluster,command,user)
      command = ["-",command].join unless command.starts_with?("-")
      command = "dfs -fs #{Hdfs.root(cluster)}/ #{command}"
      Hadoop.run(cluster,command,user)
    end

    #return the size in bytes for an Hdfs file
    def Hdfs.file_size(url,user_name)
      cluster = url.split("://").last.split("/").first
      hdfs_url = Hdfs.hdfs_url(url)
      response = Hadoop.run(cluster, "dfs -du '#{hdfs_url}'", user_name)
      if response['exit_code'] != 0
        raise "Unable to get file size for #{url} with error: #{response['stderr']}"
      else
        #parse out response
        return response['stdout'].split("\n")[1].split(" ")[1].to_i
      end
    end

    def Hdfs.read_by_dataset_path(dst_path,user_name,*args)
      cluster = dst_path.split("/").first
      url = Hdfs.url_by_path(dst_path,user_name)
      #make sure file is not too big
      if Hdfs.file_size(url,user_name) >= Hadoop.read_limit
        raise "Hadoop read limit reached -- please reduce query size"
      end
      hdfs_url = Hdfs.hdfs_url(url)
      #need to direct stderr to dev null since hdfs throws errors at being headed off
      read_command = "dfs -cat '#{hdfs_url}'"
      response = Hadoop.run(cluster,read_command,user_name)
      if response['exit_code'] != 0
        raise "Unable to read from #{url} with error: #{response['stderr']}"
      else
        return response['stdout']
      end
    end

    #used for writing strings straight up to hdfs
    def Hdfs.write_by_dataset_path(dst_path,string,user_name)
      cluster = dst_path.split("/").first
      url = Hdfs.url_by_path(dst_path,user_name)
      hdfs_url = Hdfs.hdfs_url(url)
      response = Hdfs.write(cluster,hdfs_url,string,user_name)
      if response['exit_code'] != 0
        raise "Unable to write to #{url} with error: #{response['stderr']}"
      else
        return response
      end
    end

    def Hdfs.write(cluster,hdfs_url,string,user_name)
      file_hash = {'file.txt'=>string}
      #make sure path is clear
      delete_command = "dfs -rm '#{hdfs_url}'"
      Hadoop.run(cluster,delete_command,user_name)
      write_command = "dfs -copyFromLocal file.txt '#{hdfs_url}'"
      response = Hadoop.run(cluster,write_command,user_name,file_hash)
      response
    end

    #copy file from one url to another
    #source cluster must be able to issue copy command to target cluster
    def Hdfs.copy(source_url, target_url, user_name)
      #convert aliases
      source_hdfs_url = Hdfs.hdfs_url(source_url)
      target_hdfs_url = Hdfs.hdfs_url(target_url)
      #get cluster names
      source_cluster = source_url.split("://").last.split("/").first
      target_cluster = target_url.split("://").last.split("/").first
      #delete target
      delete_command = "dfs -rm '#{target_hdfs_url}'"
      Hadoop.run(target_cluster,delete_command,user_name)
      #copy source to target
      copy_command = "dfs -cp '#{source_hdfs_url}' '#{target_hdfs_url}'"
      response = Hadoop.run(source_cluster,copy_command,user_name)
      if response['exit_code'] != 0
        raise "Unable to copy #{source_url} to #{target_url} with error: #{response['stderr']}"
      else
        return target_url
      end
    end

    # converts a source path or target path to a dst in the context of handler and stage
    def Hdfs.path_to_dst(path,stage_path,gdrive_slot)
      has_handler = true if path.index("://")
      s = Stage.where(:path=>stage_path).first
      params = s.params
      target_path = params['target']
      is_target = true if path == target_path
      red_path = path.split("://").last
      cluster = red_path.split("/").first
      #is user has a handler, is specifying a target,
      #has more than 1 slash,
      #or their first path node is a cluster name
      #assume it's an hdfs pointer
      if is_target or has_handler or Hadoop.clusters.include?(cluster) or red_path.split("/").length>2
        user_name = Hdfs.user_name_by_stage_path(stage_path)
        hdfs_url = Hdfs.url_by_path(red_path,user_name,is_target)
        return Dataset.find_or_create_by_url(hdfs_url)
      end
      #otherwise, use ssh convention
      return Ssh.path_to_dst(path,stage_path,gdrive_slot)
    end

    def Hdfs.url_by_path(path,user_name,is_target=false)
      cluster = path.split("/").first.to_s
      if Hadoop.clusters.include?(cluster)
        #cut node out of path
        path = "/" + path.split("/")[1..-1].join("/")
      else
        cluster = Hadoop.default_cluster
        path = path.starts_with?("/") ? path : "/#{path}"
      end
      url = "hdfs://#{cluster}#{path}"
      return url
    end

    def Hdfs.user_name_by_stage_path(stage_path,cluster=nil)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      user_name = s.params['user']
      cluster ||= s.params['cluster']
      cluster = Hadoop.default_cluster unless Hadoop.clusters.include?(cluster)
      node = Hadoop.gateway_node(cluster)
      if user_name and !Ssh.sudoers(node).include?(u.name)
        raise "#{u.name} does not have su permissions for node #{node}"
      elsif user_name.nil?
        user_name = u.name
      end
      return user_name
    end

    def Hdfs.write_by_stage_path(stage_path)
      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
      #return blank response if there are no slots available
      return nil unless gdrive_slot
      s = Stage.where(:path=>stage_path).first
      source = s.sources(gdrive_slot).first
      Gdrive.unslot_worker_by_path(stage_path)
      target = s.target
      cluster = target.url.split("://").last.split("/").first
      user_name = Hdfs.user_name_by_stage_path(stage_path,cluster)
      stdout = if source.handler == 'hdfs'
                   Hdfs.copy(source.url,target.url,user_name)
                 elsif ["gsheet","gfile","ssh"].include?(source.handler)
                   in_string = source.read(user_name,gdrive_slot)
                   Dataset.write_by_url(target.url, in_string, user_name)
                 end
      return {'out_str'=>stdout, 'signal' => 0}
    end
  end
end
