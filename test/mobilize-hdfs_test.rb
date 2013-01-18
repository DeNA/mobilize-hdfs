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
    hdfs_1_tsv = ([(["test"]*10).join("\t")]*10).join("\n")
    hdfs_1_sheet.write(hdfs_1_tsv,u.name)

    jobs_sheet = r.gsheet(gdrive_slot)

    test_job_rows = ::YAML.load_file("#{Mobilize::Base.root}/test/hdfs_job_rows.yml")
    jobs_sheet.add_or_update_rows(test_job_rows)

    hdfs_1_target_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1_copy.out",gdrive_slot)
    [hdfs_1_target_sheet].each {|s| s.delete if s}

    puts "job row added, force enqueued requestor, wait 120s"
    r.enqueue!
    sleep 120

    puts "jobtracker posted data to test sheet"
    test_destination_sheet = Mobilize::Gsheet.find_by_path("#{r.path.split("/")[0..-2].join("/")}/test_hdfs_1_copy.out",gdrive_slot)

    assert test_destination_sheet.to_tsv.length == 499
  end

end
