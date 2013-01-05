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

    def Hive.run_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      file_hash = Ssh.file_hash_by_stage_path(stage_path)
      hql = s.params['hql'] || file_hash.values.first
      su_user = s.params['su_user']
      Hive.run(hql, file_hash, su_user)
      out_url = "hadoop://#{s.path}"
      Dataset.find_or_create_by_url(out_url)
      out_url
    end

    def Hive.write_by_stage_path(stage_path)
      s = Stage.where(:path=>stage_path).first
      file_hash = Ssh.file_hash_by_stage_path(stage_path)

      hql = s.params['hql'] || file_hash.values.first
      su_user = s.params['su_user']
      Hive.run(hql, file_hash, su_user)


      gdrive_slot = Gdrive.slot_worker_by_path(stage_path)
      #return false if there are no emails available
      return false unless gdrive_slot
      s = Stage.where(:path=>stage_path).first
      source = s.params['source']
      target_path = s.params['target']
      source_job_name, source_stage_name = if source.index("/")
                                            source.split("/")
                                          else
                                            [nil, source]
                                          end
      source_stage_path = "#{s.job.runner.path}/#{source_job_name || s.job.name}/#{source_stage_name}"
      source_stage = Stage.where(:path=>source_stage_path).first
      tsv = source_stage.stdout_dataset.read_cache('hadoop')
      sheet_name = target_path.split("/").last
      temp_path = [stage_path.gridsafe,sheet_name].join("/")
      temp_sheet = Gsheet.find_or_create_by_path(temp_path,gdrive_slot)
      temp_sheet.write(tsv)
      temp_sheet.check_and_fix(tsv)
      target_sheet = Gsheet.find_or_create_by_path(target_path,gdrive_slot)
      target_sheet.merge(temp_sheet)
      #delete the temp sheet's book
      temp_sheet.spreadsheet.delete
      "Write successful for #{target_path}".oputs
      return true


      
    end
  end
end
