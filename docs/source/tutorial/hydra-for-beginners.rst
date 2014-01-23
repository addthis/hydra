.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _hydra-for-beginners:

######################
Introduction to Hydra
######################


Module 1
==========

Introduction
--------------

Welcome to the Hydra tutorial.  This series of modules will walk you through the basics of Hydra in order to help you understand the system and exploit its capabilities.  The intended audience for this tutorial are Hydra users.  The focus will be on how to develop Hydra jobs and queries to perform tasks (simple to complex).  Hydra is a distributed system and some basic knowledge about how that system works is crucial to using it efficiently so this tutorial will cover some of the core design elements as well.

Although Hydra supports a variety of data analysis processes it original goal and most common use case is time-series analysis.   An event occurs with a certain set of features at a certain time and we'd like to answer questions like:

1.  How many events occurred in time range R for publisher P and domain D?
2.  Which user agents did we see most often in time range R?
3.  How many unique IDs did we see in a certain time range?

To support this type of time-series analysis efficiently Hydra's primary data structure is a Tree.  Most databases use a tree data structure to create indexes on data tables.  Hydra uses the tree (index) as the data structure.  This allows Hydra to aggregate and analyze data on write and reduces the amount of data that must be stored long term.  Updating the analysis on write is a critical feature for Hydra that allows incremental updates to the data structure and fast analysis without massive data processing at query time.  

In addition to time-series analysis Hydra can also be used for key-value data analysis.  For example mapping ID between two namespaces requires a data structure where you can ask questions like, "Given a ID from namespace X what is the matching ID for namespace Y?".

The key to both time-series and key-value data analysis in Hydra is a solid understanding of how the distributed system works, how the data is distributed across the system, and how to select efficient data structures for both reads and writes.

Working with systems like Hydra is different than working with traditional RDBMS data stores. In a traditional data store much of the upfront design effort is spent on designing a data schema that is normalized and generic enough to answer a wide variety of potential queries.  For example you may have a table with basic USER information and then an EVENT table that has a USER_ID column that can be used to map events to a given user.  In Hydra the question we think about the question we'd like to ask first and then build a data structure to help answer that question.  The data is often denormalized (meaning we optimize for read performance and often duplicate the data to make reads more efficient).  It is also very common to process the same source data into several different jobs with different data structures so that each job is optimized for the specific query(s) it is designed to answer.

Goals for this Module:

- Understand the basics Hydra and the types of problems it is well suited for
- Understand the key aspects of Hydra's design that make it unique

Problem Scope
--------------

Hydra is a large-scale distributed stream processing system.  Hydra can be thought of as three distinct systems that together create a powerful distributed system that is capable of scaling to hundreds (at least) of servers.  The three distinct systems are:

1.  A distributed file serving system
2.  A distributed stream processing system
3.  A distributed query system

In this tutorial we will learn about each of these systems and how they can be combined to perform amazing feats of data manipulation at scale.

Hydra was built to solve a specific type of 'big data' problem.  How can very large data streams (often greater than 100s of billions of events) be processed and analyzed continuously in a performant and cost efficient way.

Challenges At Large Scale
--------------------------

Distributed computing is just like normal computing only harder!  By definition distributed computing requires spreading the data and computation across multiple machines (2 or more). Whenever a process is spread out of multiple machines the probability of failure goes up.  The more machines participating in a cluster the more likely a failure will occur.  For this reason the biggest challenge for a distributed system is to work reliably in an environment where hardware failures are common.  To put this in perspective think about a system that fails 1 out of every 100 attempts.  If you submit 100 jobs to that system 1/100th of them may fail.  Not great but the large majority of the time everything works.  If that same problem is distributed to a distributed system of 100 servers where each fails about 1 out of 100 attempts then the error rate from the user's perspective jumps to *63%*.  So the majority of job submissions will fail.  This means that the distributed system needs to anticipate failure and autonomically recover from those failures so that they are hidden from the end user.  

Another major challenge with large scale computing is evenly balancing the load across all available resources.  Depending on the algorithm used to partition data across the systems data 'hotspots' can cause unbalanced loads.  Even if a good distribution algorithm is used that evenly distributes data the individual performance characteristics of each server can cause some servers to be faster than others.  When machines fail the data stored on those systems must be re-balanced to other servers and this can lead performance issues.  Adding or removing servers from a cluster is another opportunity to introduce imbalance.  Distributed systems need to understand the cost of this imbalance and make decisions about when it is worthwhile to move the data and processing to available nodes. 

The only thing worse than a server failing in a distributed system is a server slowly dying in a distributed system.  A server that is dying but not dead can cause outsized impact on the overall cluster performance.  These conditions must be detected and the impacted machines must be avoided in order to prevent service degradation.

In the 90s and early 2000s RAID was the answer to all IO related performance and redundancy needs.  With the surge in popularity of distributed systems RAID are not sufficient or practical.  The goal is to use commodity servers that have a much higher failure rate than single server systems of the past.  Without RAID to protect the consistency and reliability of our data it is up to the distributed system to ensure that data is protected from failure.  The typical solution to this problem is to ensure that *n* copies of the data are available on n different servers.  Some solutions draw from information theory to reduce the storage overhead required to store *n* replicas but those systems rely on very high performance networking to be feasible.


What Hydra Is Not Good At
--------------------------

Hydra is not a traditional relational data store  `RDBMS <http://en.wikipedia.org/wiki/Relational_database_management_system>`_.  It does not support transactions (or rollbacks) and it does not support SQL queries.  Hydra was designed for high performance data processing

How Hydra Works
----------------

Hydra is designed to operate on a large number of commodity (commodity does not mean cheap) servers rather than a small number of enterprise class servers.  This approach 
is more cost effective and can be more easily scaled up or down than a comparable big iron approach to the problem.

Hydra servers are broken into two classes.  Cluster Heads and Minions.  There are a small number of Cluster Heads in a given cluster (usually 2 or 3) and these servers act as gateways to the (typically) large number of Minion servers that are the computation servers.

For data processing Hydra uses the **job** as an abstraction for the work to be performed.  A job will be split into *n* **tasks** and each task represents a unit of work.  A job with 10 tasks would have 10 individual processes that each performs roughly 1/10th of the overall workload.  When the job is submitted Hydra will allocate the tasks for the job to available Minions.  It is possible for multiple tasks of the same job to run on a single Minion.  

When a minion performs the work for a task the data generated is stored initially on the local file system.  When the task completes the data will be replicated to *r* servers where *r* is the replication factor.  On subsequent runs of the task any server with the original or copies of the data for that task may be selected to process the new data.

Hydra supports incremental data processing.  A task will run until there is no more available data or a time limit is reached.  The next time that tasks runs it will start processing from where it left off.  This is exactly what you need when your source is a stream (think apache access logs).

Thinking In Trees
------------------

To be a successful Hydra user you need to learn to 'think in trees'.  To understand what it means to think in trees first we must consider the traditional data modeling approach.  In a traditional database we typically think about the data model first.  The goal is to build a normalized data model that is very flexible and does not duplicate data across tables.  So for example if we have users and events we might create one table with user data and another table with events.  Later when we want to get all of the events for a given user we would join those two tables (hopefully on indexed columns) to extract our results. 

In Hydra the recommended approach is to first think about the query and then build the data model to support that query.  It is not uncommon to process the same source data multiple times in order to support alternative data structures.  The primary data structure in Hydra is a tree.  To provide a concrete example lets suppose that we want to analyze our apache access logs to answer the question, "How many unique IP addresses did my web servers accept connections from for each domain that they serve?".  In addition to information about the total number of unique IP addresses we also want to be able to break down that information by day.  Now that we know the question we can think about the data structure that efficiently provides access to the desired answers.  Here the anser is straight forward.  A tree like the following:

-  root
    -  DOMAIN:IPCOUNTER
        - DATE:IPCOUNTER
    - DATE:IPCOUNTER
    

The power of this tree is that the number of nodes only grows when we see a new date in the input stream.  It grows as a function of the number of unique dates not the number of records in the input stream.  This is an innate feature of tree-based data structures and it allows Hydra users to represent vast quantities of data efficiently.   

Data Distribution
------------------

Each Minion in a Hydra cluster acts as a data repository as well as a compute node.  Hydra splits large input streams across the cluster.  The job designer determines how many servers to store the data on and how many partitions the data should be split into.  The participating servers use a common algorithm to decide which servers should process what subset of the input stream.  After selecting the appropriate subset of the stream the server will partition the stream into *p* partitions and write those partitions to disk.  Like Hadoop, Hydra works primarily with record-oriented data streams.  

In addition to consuming data from external sources some Hydra jobs generate their own data sets.  We call these jobs **Map Jobs**.  Like split jobs, map jobs run on an arbitrary number of Minion servers with each process consuming a subset of the total data stream.  The output of a Map job is a tree-based data structure that is stored on the local machine where the processing occurred.  When a process reaches a checkpoint the data written to the local file system for the Map job is replicated to *r* servers.  Hydra tracks where these replicas exist and the next time this process is initiated it will run on one of the servers that already has the data.

When a Map job or a Split job consumes data stored in a Hydra cluster it will discover the set of of servers that have relevant data and then stream the data from those servers on demand.  This is an alternative approach to Hadoop where all computation occurs on the same machine that contains the data.  We have found that using intelligent streaming algorithms and moving compressed data across the network we can achieve excellent performance and reduce the need for many data preparation jobs to move all relevant data into a single chunk on a single server before performing the desired computations.

Like computation hot spots data hot spots are something that distributed systems like Hydra need to handle.  Hydra will periodically re-balance the data in the cluster moving data from machines running low on disk space to servers that have excess capacity.  

Scalability
------------

Hydra is a system designed to scale out and up.  This means that we can scale Hydra either by adding more servers to a cluster or by increasing the capacity of the servers already in the cluster.  Currently we've run Hydra clusters with 100+ servers.  In theory we should be able to scale much larger than that but we have not had the need or capacity to do so.

Query System
--------------

TODO

The rest of the tutorial
-------------------------

The introduction focused on high level concepts to give you a general idea of how Hydra works the type of problems it was built to solve.  The rest of the tutorial is designed to show you had to build jobs and write queries to make use of the system.

- In :ref:`Module2 <sources>` you will learn about configuring data sources for your jobs
- In :ref:`Module3 <sinks>` you will learn how to select and configure Hydra sinks (outputs) for your jobs
- In :ref:`Module4 <bundle-filters>` you will learn about bundle filters and how they can be used to manipulate your data
- In :ref:`Module5 <value-filters>` you will learn about value filters and how they can be used to manipulate your data
- In :ref:`Module6 <split-job>` you will learn how to write a Split Job. 
- In :ref:`Module7 <map-job>` you will learn how to write a Map Job.

..
   - In :ref:`Module8 <basic-query>` you will learn how to write a query to extract information from the job you built in [Module7](/dev/hydra_training_map_job)
   - In [Module9](/dev/hydra_training_partitions) you will learn how to think strategically about how to partition your data in the cluster
   - In [Module10](/dev/hydra_training_sampling) you will learn how to use sampling to improve performance when accuracy is not paramount
   - In [Module11](/dev/hydra_training_data_attachments) you will learn about data attachments and how they can used to efficiently track key characteristics about your data stream with relatively low overhead
   - In [Module12](/dev/hydra_training_cardinality_estimation) you will learn about cardinality estimation utilities in Hydra and how to select and configure the right estimator for your use case
   - In [Module13](/dev/hydra_training_performance_tuning) you will learn about advanced performance tuning best practices
   - In [Module14](/dev/hydra_training_cassandra_integration) you will learn how to integrate Hydra jobs with Cassandra
   - In [Module15](/dev/hydra_training_meshy) you will learn about Meshy.  The mesh based distributed network protocol used by Hydra
   - In [Module16](/dev/hydra_training_muxy) you will learn about Muxy a file system abstraction layer that makes it easier to work with a very large number of files
 
