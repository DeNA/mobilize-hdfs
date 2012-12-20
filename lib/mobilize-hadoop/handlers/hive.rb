module Mobilize
  module Hive
    def config
      Hadoop.config['hive']
    end

    def Hive.exec_path
      Hive.config['exec_path']
    end

    def Hive.run(hql,file_hash,su_user=nil)
      hql_md5 = hql.to_md5
      file_hash = {hql_md5 => hql}
      h_command = "#{Hive.exec_path} -f #{hql_md5}"
      Ssh.run(Hadoop.node,h_command,file_hash,su_user)
    end

    def Hive.run_by_task_path(task_path)
      return false unless gdrive_slot
      t = Task.where(:path=>task_path).first
      file_hash = Ssh.file_hash_by_task_path(task_path)
      hql = t.params['hql'] || file_hash.values.first
      su_user = t.params['su_user']
      Hive.run(hql, file_hash, su_user)
    end
  end
end
