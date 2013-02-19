Mobilize-Hdfs
===============

Mobilize-Hdfs adds the power of hdfs to [mobilize-ssh][mobilize-ssh].
* read, write, and copy hdfs files through Google
Spreadsheets.

Table Of Contents
-----------------
* [Overview](#section_Overview)
* [Install](#section_Install)
  * [Mobilize-Hdfs](#section_Install_Mobilize-Hdfs)
  * [Install Dirs and Files](#section_Install_Dirs_and_Files)
* [Configure](#section_Configure)
  * [Hadoop](#section_Configure_Hadoop)
* [Start](#section_Start)
  * [Create Job](#section_Start_Create_Job)
  * [Run Test](#section_Start_Run_Test)
* [Meta](#section_Meta)
* [Author](#section_Author)

<a name='section_Overview'></a>
Overview
-----------

* Mobilize-hdfs adds Hdfs methods to mobilize-ssh.

<a name='section_Install'></a>
Install
------------

Make sure you go through all the steps in the
[mobilize-base][mobilize-base] and [mobilize-ssh][mobilize-ssh]
install sections first.

<a name='section_Install_Mobilize-Hdfs'></a>
### Mobilize-Hdfs

add this to your Gemfile:

``` ruby
gem "mobilize-hdfs"
```

or do

  $ gem install mobilize-hdfs

for a ruby-wide install.

<a name='section_Install_Dirs_and_Files'></a>
### Dirs and Files

### Rakefile

Inside the Rakefile in your project's root dir, make sure you have:

``` ruby
require 'mobilize-base/tasks'
require 'mobilize-ssh/tasks'
require 'mobilize-hdfs/tasks'
```

This defines rake tasks essential to run the environment.

### Config Dir

run

  $ rake mobilize_hdfs:setup

This will copy over a sample hadoop.yml to your config dir.

<a name='section_Configure'></a>
Configure
------------

<a name='section_Configure_Hadoop'></a>
### Configure Hadoop

* Hadoop is big data. That means we need to be careful when reading from
the cluster as it could easily fill up our mongodb instance, RAM, local disk
space, etc.
* To achieve this, all hadoop operations, stage outputs, etc. are
executed and stored on the cluster only. 
  * The exceptions are:
    * writing to the cluster from an external source, such as a google
sheet. Here there
is no risk as the external source has much more strict size limits than
hdfs.
    * reading from the cluster, such as for posting to google sheet. In
this case, the read_limit parameter dictates the maximum amount that can
be read. If the data is bigger than the read limit, an exception will be
raised.

The Hadoop configuration consists of:
* output_cluster, which is the cluster where stage outputs will be
stored. Clusters are defined in the clusters parameter as described
below.
* output_dir, which is the absolute path to the directory in HDFS that will store stage
outputs. Directory names should end with a slash (/).
* read_limit, which is the maximum size data that can be read from the
cluster. This is applied at read time by piping hadoop dfs -cat | head
-c <size limit>. Default is 1GB.
* clusters - this defines aliases for clusters, which are used as
parameters for Hdfs stages. Cluster aliases contain 5 parameters:
  * namenode - defines the name and port for accessing the namenode
    * name - namenode full name, as in namenode1.host.com
    * port - namenode port, by default 50070
  * gateway_node - defines the node that executes the cluster commands.
  * exec_path - defines the path to the hadoop 
This node must be defined in ssh.yml according to the specs in
[mobilize-ssh][mobilize-ssh]. The gateway node can be the same for
multiple clusters, depending on your cluster setup.

Sample hadoop.yml:

``` yml
---
development:
  output_cluster: dev_cluster
  output_dir: /user/mobilize/development/
  read_limit: 1000000000
  clusters:
    dev_cluster:
      namenode:
        name: dev_namenode.host.com
        port: 50070
      gateway_node: dev_hadoop_host
      exec_path: /path/to/hadoop
    dev_cluster_2:
      namenode:
        name: dev_namenode_2.host.com
        port: 50070
      gateway_node: dev_hadoop_host
      exec_path: /path/to/hadoop
test:
  output_cluster: test_cluster
  output_dir: /user/mobilize/test/
  read_limit: 1000000000
  clusters:
    test_cluster:
      namenode:
        name: test_namenode.host.com
        port: 50070
      gateway_node: test_hadoop_host
      exec_path: /path/to/hadoop
    test_cluster_2:
      namenode:
        name: test_namenode_2.host.com
        port: 50070
      gateway_node: test_hadoop_host
      exec_path: /path/to/hadoop
production:
  output_cluster: prod_cluster
  output_dir: /user/mobilize/production/
  read_limit: 1000000000
  clusters:
    prod_cluster:
      namenode:
        name: prod_namenode.host.com
        port: 50070
      gateway_node: prod_hadoop_host
      exec_path: /path/to/hadoop
    prod_cluster_2:
      namenode:
        name: prod_namenode_2.host.com
        port: 50070
      gateway_node: prod_hadoop_host
      exec_path: /path/to/hadoop
```

<a name='section_Start'></a>
Start
-----

<a name='section_Start_Create_Job'></a>
### Create Job

* For mobilize-hdfs, the following stages are available. 
  * cluster and user are optional for all of the below.
    * cluster defaults to output_cluster;
    * user is treated the same way as in [mobilize-ssh][mobilize-ssh].
  * hdfs.read `source:<hdfs_full_path>, user:<user>`, which reads the input path on the specified cluster.
  * hdfs.write `source:<gsheet_full_path>, target:<hdfs_full_path>, user:<user>` 
  * hdfs.copy `source:<source_hdfs_full_path>,target:<target_hdfs_full_path>,user:<user>`
  * The gsheet_full_path should be of the form `<gbook_name>/<gsheet_name>`. The test uses "Requestor_mobilize(test)/test_hdfs_1.in".
  * The hdfs_full_path is the cluster alias followed by full path on the cluster. 
    * if a full path is supplied without a preceding cluster alias (e.g. "/user/mobilize/test/test_hdfs_1.in"), 
      the output cluster will be used.
    * The test uses "/user/mobilize/test/test_hdfs_1.in" for the initial
write, then "test_cluster_2/user/mobilize/test/test_hdfs_copy.out" for
the copy and subsequent read.
  * both cluster arguments and user are optional. If copying from
one cluster to another, your source_cluster gateway_node must be able to
access both clusters.

<a name='section_Start_Run_Test'></a>
### Run Test

To run tests, you will need to 

1) go through the [mobilize-base][mobilize-base] and [mobilize-ssh][mobilize-ssh] tests first

2) clone the mobilize-hdfs repository 

From the project folder, run

3) $ rake mobilize_hdfs:setup

Copy over the config files from the mobilize-base and mobilize-ssh
projects into the config dir, and populate the values in the hadoop.yml file.

If you don't have two clusters, you can populate test_cluster_2 with the
same cluster as your first.

3) $ rake test

* The test runs a 4 stage job:
  * test_hdfs_1:
    * `hdfs.write target:"/user/mobilize/test/test_hdfs_1.out", source:"Runner_mobilize(test)/test_hdfs_1.in"`
    * `hdfs.copy source:"/user/mobilize/test/test_hdfs_1.out",target:"test_cluster_2/user/mobilize/test/test_hdfs_1_copy.out"`
    * `hdfs.read source:"/user/mobilize/test/test_hdfs_1_copy.out"`
    * `gsheet.write source:"stage3", target:"Runner_mobilize(test)/test_hdfs_1_copy.out"`
  * at the end of the test, there should be a sheet named "test_hdfs_1_copy.out" with the same data as test_hdfs_1.in

<a name='section_Meta'></a>
Meta
----

* Code: `git clone git://github.com/dena/mobilize-hdfs.git`
* Home: <https://github.com/dena/mobilize-hdfs>
* Bugs: <https://github.com/dena/mobilize-hdfs/issues>
* Gems: <http://rubygems.org/gems/mobilize-hdfs>

<a name='section_Author'></a>
Author
------

Cassio Paes-Leme :: cpaesleme@dena.com :: @cpaesleme

[mobilize-base]: https://github.com/dena/mobilize-base
[mobilize-ssh]: https://github.com/dena/mobilize-ssh
