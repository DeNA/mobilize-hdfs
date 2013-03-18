require 'test_helper'

describe "Mobilize" do

  def before
    puts 'nothing before'
  end

  # enqueues 4 workers on Resque
  it "runs integration test" do

    puts "restart workers"
    Mobilize::Jobtracker.restart_workers!

    gdrive_slot = Mobilize::Gdrive.owner_email
    puts "create user 'mobilize'"
    user_name = gdrive_slot.split("@").first
    u = Mobilize::User.where(:name=>user_name).first
    r = u.runner
    hdfs_1_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1.in",gdrive_slot)
    [hdfs_1_sheet].each {|s| s.delete if s}

    puts "add test_source data"
    hdfs_1_sheet = Mobilize::Gsheet.find_or_create_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1.in",gdrive_slot)
    hdfs_1_tsv = ([%w{test0 test1 test2 test3 test4 test5 test6 test7 test8 test9}.join("\t")]*10).join("\n")
    hdfs_1_sheet.write(hdfs_1_tsv,u.name)

    jobs_sheet = r.gsheet(gdrive_slot)

    test_job_rows = ::YAML.load_file("#{Mobilize::Base.root}/test/hdfs_job_rows.yml")
    test_job_rows.map{|j| r.jobs(j['name'])}.each{|j| j.delete if j}
    jobs_sheet.add_or_update_rows(test_job_rows)

    hdfs_1_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1_copy.out",gdrive_slot)
    [hdfs_1_target_sheet].each {|s| s.delete if s}

    puts "job row added, force enqueued requestor, wait for stages"
    r.enqueue!
    wait_for_stages

    puts "jobtracker posted data to test sheet"
    test_destination_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1_copy.out",gdrive_slot)

    assert test_destination_sheet.read(u.name).length == 599
  end

  def wait_for_stages(time_limit=600,stage_limit=120,wait_length=10)
    time = 0
    time_since_stage = 0
    #check for 10 min
    while time < time_limit and time_since_stage < stage_limit
      sleep wait_length
      job_classes = Mobilize::Resque.jobs.map{|j| j['class']}
      if job_classes.include?("Mobilize::Stage")
        time_since_stage = 0
        puts "saw stage at #{time.to_s} seconds"
      else
        time_since_stage += wait_length
        puts "#{time_since_stage.to_s} seconds since stage seen"
      end
      time += wait_length
      puts "total wait time #{time.to_s} seconds"
    end

    if time >= time_limit
      raise "Timed out before stage completion"
    end
  end  

end
