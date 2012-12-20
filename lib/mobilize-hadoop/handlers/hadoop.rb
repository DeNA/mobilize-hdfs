module Mobilize
  module Hadoop
    def Hadoop.config
      Base.config('hadoop')
    end

    def Hadoop.exec_path
      Hadoop.config['hdfs']['exec_path']
    end

    def Hadoop.node
      Hadoop.config['node']
    end

    def Hadoop.run(command,file_hash={},su_user=nil)
      h_command = if command.starts_with?("hadoop")
                    command.sub("hadoop",Hadoop.exec_path)
                  else
                    "#{Hadoop.exec_path} #{command}"
                  end
      Ssh.run(Hadoop.node,h_command,file_hash,su_user)
    end

    def Hadoop.fs(command,file_hash={},except=true,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      Hadoop.run("fs #{command}",file_hash,except,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hadoop.ls(command="")
      Hadoop.fs("ls #{command}")
    end

    def Hadoop.cat(path,su_user=nil)
      #ignore errors due to mising file
      Hadoop.fs("cat #{path}",{},false)
    end

    def Hadoop.rm(path,su_user=nil)
      #ignore errors due to missing file
      Hadoop.fs("rm #{path}",{},false,su_user)
    end

    def Hadoop.rmr(dir,su_user=nil)
      #ignore errors due to missing dir
      Hadoop.fs("rmr #{dir}",{},false,su_user)
    end

    def Hadoop.job(command,file_hash={},except=false,su_user=nil)
      command = ["-",command].join unless command.starts_with?("-")
      Hadoop.run("job #{command}",file_hash,except,su_user).ie do |r|
        r.class==Array ? r.first : r
      end
    end

    def Hadoop.working_jobs
      raw_list = Hadoop.job("list")
      raw_list.split("\n")[1..-1].join("\n").tsv_to_hash_array
    end

    def Hadoop.job_status(hdfs_job_id)
      raw_status = Hadoop.job("status #{hdfs_job_id}")
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

    def Hadoop.job_params(hdfs_job_id)
      Hash.from_xml(Hadoop.job("status #{hdfs_job_id}"))
    end

    def Hadoop.read(path)
      #read contents of folder
      path += "*" if path.ends_with?("/")
      Hadoop.cat("#{path}")
    end

    def Hadoop.write(path,string,su_user=nil)
      file_hash = {'file.txt'=>string}
      Hadoop.rm(path) #remove old one
      write_command = "fs -copyFromLocal file.txt #{path}"
      Hadoop.run(write_command,file_hash,true,su_user)
      return true
    end

    def Hadoop.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      command = params['cmd']
      file_hash = Ssh.file_hash_by_stage_path(stage_path)
      su_user = s.params['su_user']
      if su_user and !Ssh.sudoers(Hadoop.node).include?(u.name)
        raise "You do not have su permissions for this node"
      elsif su_user.nil? and Ssh.su_all_users(node)
        su_user = u.name
      end
      Hadoop.run(node,command,file_hash,su_user)
    end

    def Hadoop.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      u = s.job.runner.user
      params = s.params
      target = params['target']
      file_hash = Ssh.file_hash_by_stage_path(stage_path)
      string = file_hash.values.first
      su_user = s.params['su_user']
      if su_user and !Ssh.sudoers(Hadoop.node).include?(u.name)
        raise "You do not have su permissions for this node"
      elsif su_user.nil? and Ssh.su_all_users(node)
        su_user = u.name
      end
      Hadoop.write(target,string,su_user)
    end
  end
end
