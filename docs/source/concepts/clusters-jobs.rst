.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _clusters-jobs:

##################
Clusters and Jobs
##################

So every distributed execution service has it's own variant on terminology (is it a box? host? node?). This is Hydra's. There is a central orchestrator called Spawn.  Spawn is in charge of cluster meta-data and job management (what should run where, when).  Minions actually perform the work of the cluster.  They execute the jobs, watch them run, tell Spawn how it went, etc.  By convention in a production cluster there is one minion per host. Multiple minions per host are hostful for contrived testing or some specialized JBOD setups.  All worker nodes are colloquially called minions (so ``minion`` the java process and minion the box type).

Commands and Jobs
=================

Each cluster has a set of Commands.  Commands reference shell scripts (usually we copy them to every host, but you could run them off of NFS or anything else you can mount), along with some constraints like "this command needs X CPU and Y memory.  In practice most commands are just banal ceremony around running java programs but they can be arbitrary shell scripts [#]_.  A typical example would look like::

  ${HOME}/scripts/job-task.sh job.conf {{nodes}} {{node}} {{jobid}}

Where ``job-task.sh`` is just a plain old shell script (in this case it's mostly just a collection of JVM flags).  The node and job information in brackets is templated in, see the section on :ref:`sharding <shards-pipelines>` for the details.

Each job is defined by its command, the configuration you provide, the number of :ref:`shards <shards-pipelines>`, how long the job should run, etc.   Jobs are divided into a number of *tasks*.  These tasks are then allocated among the minions.  A minion may end with multiple tasks from the same job (if that is best for cluster health as a whole, or if that job has more tasks than the total minions) but usually tasks are spread around.

If your job is interesting, you probably want to keep it running so you find out what's new every day.  Jobs can be scheduled to run again or *kick* periodically.  Spawn will take care making sure your job is submitted again within however many minutes you specify.  Keep in mind there is no way to guarantee that your job will not have to wait in line behind other jobs.  To keep a (perhaps inadvertently) greedy job from taking all of the clusters resources (keeping *you* from running your totally rad new job), it is a good practice to set a maximum time for your job to run before others get a turn.  You may also give a job priority that will cause it to wait in line for less time, but being a good cluster citizen is preferable to creating a complex ontology of job priority.

To support pipelines you can have jobs trigger the execution of other jobs on successful completion.

.. [#] We have used this to run a Cassandra cluster inside a Hydra cluster for testing.  Perhaps one day we will run Hydra inside Hydra to expand our set of Russian dolls.

Backups and Replicas
====================

A fundamental (tbd-ref) part of all hydra jobs is their reliance on checkpointing.  The results of a job would not be visible to other jobs until a successful checkpoint is triggered on job completion.  That means:
 1. The job shut down cleanly [#]_.
 2. Update replicas.
 3. Update backup.
 4. Switch a symlink to point to the last good backup.

.. [#] Think Unixy everyone.  Success means processes that return zero.

This nomenclature has a specific meaning.  Backups are *local* to the host.  They protect against (some) human mistakes but not machine failure.  Replicas are on remote hosts.  They are where you look to recover when a host fails.  You can configure how many replicas are created and how many daily etc. backups are created.  Replicas are created using good old ``rsync`` while backups ``LD_PRELOAD`` a spiffy userspace `copy-on-write library <https://github.com/hermanjl/flcow-osx/>`_.  Both backups and replicas are *incremental*.


Splitting Trees
================

So far we have talked about jobs in the abstract and emphasized the potential for them to be arbitrary programs.  However, the archtypical Hydra jobs are of two varieties:

 * Split jobs take in lines of data (think log files) and emit new lines.  It might change the format (text in, binary out), drop lines that fail to some predicate, create multiple derived lines from each input, make all strings lowercase, or other arbitrary transformations.  But it's always lines in, lines out.
 * TreeBuilder (or Map) jobs take in log lines of input (such as just emitted by a split job!) and build a :ref:`tree <paths-trees>`  representation of the data.  This hierarchical databases can then be explored through the distribute Hydra :ref:`query system<queries>`.


Other Friends
=============

The combination of Spawn and Minion provide job cluster wide meta-data, job management, and command and control.  They do this by using `Apache Zookeeper`_ to store cluster data [#]_ and `rabbitmq`_ to issue command and notifications.  In addition each minion node also has the following processes:
 * `meshy`_ : A distributed file server which provides high-throughput access to any file in the cluster over multiplexed streams.  You can think of it as a read only distributed file system.
 * QueryWorker: The scatter part of the scatter-gather Hydra :ref:`query system<queries>`.

Ideally during operation node of these support processes would be using significant resources, while the actual job's launched by minion would be tearing through data.

.. _Apache Zookeeper: http://zookeeper.apache.org/

.. _rabbitmq: http://www.rabbitmq.com/

.. _meshy: http://github.com/addthis/meshy/

.. [#] In addition to providing redundancy, failover, and all sorts of other helpful services for building distributed systems.



Colocation
------------

Usually we run minion, queryworker, and meshy file server on every worker node.  And Spawn, QueryMaster, and ZooKeeper on a trifecta of redundant cluster heads.  While the minion colocation is necessary to have a fully functional cluster you can spread the master processes as you see fit.


