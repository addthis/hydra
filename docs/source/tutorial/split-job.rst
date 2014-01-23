.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _split-job:

##########
Split Job
##########

Motivation
============

Hydra processes log-structured data on a cluster of servers. The incoming stream of data may be *split* into partitions so that each individual partition can be stored on one of the cluster machines for further processing. In Hydra the splitting of data is performed by writing a split job. By writing a split job the Hydra user has the opportunity to partition the input stream in such a way so that similar data rows will be deposited on the same server. With the appropriate partition scheme it is possible to improve further processing of the data set. Further processing is performed in the :ref:`map job <map-job>`

Split Job Configuration
-------------------------

A Hydra split job and Hydra map job both have three top-level sections: `source`, `map`, and `output`. `source` specifies the input stream for the job. `map` specifies a sequence of transformations that are performed on the input stream. `map` transformation are performed one a row-by-row basis. Rows are processed approximately in sequential order but not strictly in sequential order. `output` specifies the output location after the input stream has been transformed. The only difference between a Hydra split job and Hydra map job is the specification of the `output` section. Split jobs write to an output stream, specifically they write to a sequence of files that will be consumed by one or more map jobs. Below is an example split job.

Example A:

.. code-block:: javascript

    {
      type : "map",
      source : {
        type : "empty",
        maxPackets : 100000,
      },
      map : {
        fields : [],
        filterOut :
          {op:"map", fields:[
            {from:"counter", filter: {op:"count", format:"0000000"}},
            {from: "DATE_YMD", filter: {op: "time-range", now:true, format:"YYMMdd"}},
          ]},
      },
      output : {
        type : "file",
        path : ["{{DATE_YMD}}", "/", "[[node]]"],
        writer : {
          maxOpen : 1024,
          flags : {
            dir : "split",
            multiplex : true,
            compress : true,
          },
          format : {
            type : "channel"
          }
        }
      }
    }

Let's walk through several features of the split job:

 -   A split job begins with ``type: "map"``. A map job also begins with the same designation. It is confusing but that's how it works so just get used to it.
 -   For this example we are using an empty input source. The empty input source generates a predetermined number of empty bundles. The empty input source is not very useful but it is instructional for teaching purposes. 
 -   Under the ``output`` --> ``writer`` --> ``flags`` section are some important properties.
 -   The "dir" parameter specifies the root directory for the output files. By convention the "dir" parameter is assigned the directory "split".
 -   The "multiplex" parameter enables generating a small number of large files to represent a large number of small files. It is a very good idea to set "multiplex" to true.
 -   The "compress" parameter applies file compression on the output. This is generally a good idea.

Selective Input and Output
----------------------------

One of the primary responsibilities of the split job is to partition the input stream across the nodes of the cluster.

Selective Input
---------------------

There are two primary mechanisms for partitioning the input stream: the mod mechanism and the hash mechanism. In the mod mechanism the total number of shards from the input stream must be known. Or in other words, using the mod mechanism requires using the 'shardTotal' parameter. Let's look at an example. In the example below we know that the upstream job with job id c2116970-23b0-4c4d-bf22-c746441dd1a0 has split the data into a total of 64 shards. This job can run with 1 to 64 nodes and the data will be distributed across those nodes.

Example B:

.. code-block:: javascript

	source : {
	    type : "stream2",
	    shardTotal : 64,
	    source : {
		startDate : "{{now-2}}",
		endDate : "{{now}}",
		files : [{
			dir : "*/c2116970-23b0-4c4d-bf22-c746441dd1a0/*/gold/split/{Y}{M}{D}/*/",
			match : [".*/gold/split/{Y}{M}{D}/.*/{{mod}}-[0-9][0-9][0-9].*gz"],
		}],
	    },
	    factory : {
		type : "channel",
		input : {type : "inject"},
	    },
	}

The hash mechanism makes no assumptions about the input stream and tries to evenly distribute the data across the tasks in the job.

Example C:

.. code-block:: javascript

	source : {
	    type : "stream2",
	    hash : true,
	    source : {
		startDate : "{{now-6}}",
		endDate : "{{now}}",
		sortToken : "/",
		sortTokenOffset : 3,
		hosts : [
			{host : "asf01", port : 1337},
		],
                files : [
                    {dir : "iad/share-logs/{YY}/{M}/{D}/", match : [".*-{Y}{M}{D}-[0-9]+.*gz"]},
                    {dir : "lax/share-logs/{YY}/{M}/{D}/", match : [".*-{Y}{M}{D}-[0-9]+.*gz"]},
                ],
	    },
	    factory : {
		type : "kv.txt",
		source : {
			type : "legacy",
			source : {type : "inject"},
		},
	    },
	}

Selective Output
-----------------

The most important decision the designer of a split job makes is the selection of the shard key. This will influence
how evenly the data is distributed in the cluster, the efficiency of downstream jobs to process data, and the
efficiency of queries that run against downstream jobs.  Lets consider each of these elements individually.

When selecting your shard key (can also be called partition) you should consider how it will impact data
distribution among the output files. In an ideal world each output file has exactly the same amount of data.
This means that each downstream task processing data generated by your split job would have an equal amount of work to do.

Suppose you pick *domain* as the shard key.  There may be very good reasons to shard data this way. For example your
downstream job may need all of the data for a given domain on a single node.  Sharding by domain is the only
way to accomplish that.  However sharding this way will create an unbalanced data distribution.
Consider `www.cnn.com` vs `www.yourfriendsblog.com`.  CNN is going to have a ton of data but your friend's blog will only
have small amount.

Efficiency of downstream jobs is impacted by data distribution.  Imagine that you have two shards and one shard gets
99% of the data.  Since your downstream job can only run 2 tasks (since there are only 2 shards) than 1 task (process)
will take 99% longer than the other task to complete.  This can lead to the `long tail <http://en.wikipedia.org/wiki/Long_tail>`_
problem where your system has spare capacity but it cannot be used because of the data imbalance.

Another factor in downstream job efficiency is dependent on the choice of shard key and the structure of the map job
that is consuming the split data.  Consider a map job that creates a tree that stores the top 100 URLs for each domain.
If the input data is sharded by domain than every task in the map job will only have the domains for the
shard keys that it is processing.  For example if there are two partitions and 100 domains than each partition will
have 50 domains.  By sharding the data this way it makes it feasible that the map jobs will be able to fit each
individual data set into memory and the resulting job runtime will be much faster than if it were to spill to disk.
If you took the same data and sharded by a key other than domain than each partition would have all 100 domains.
Taken to extremes if you have a job that stores data by domain and you process our full data set (15 million domains
at this time) then every data set has all 15M domains.

The depth of your tree is a critical factor in the performance of the map job.  For this reason it often makes sense
to split the same raw data multiple times but sharded by different keys for different consumers.

Another thing about understanding the partition key is that you can take advantage of bloom filters and fast fail to
improve the efficiency of a query.  For example if you know the data is sharded by publisher id than you can put a
bloom filter at the top of your map job that tracks publisher id.  When you run your query you can check that bloom filter
for the publisher id you are searching for and if the bloom does not detect your pub it fails immediately without having to
descend into the child nodes (possibly millions of them) to find the key you are looking for.

Here is the output section of a split job that feeds into the input shown in (Example C). Notice how the hash value that is computed is modulo 64 which dictates the shard total in the Example C.

Example D:

.. code-block:: javascript

	map : {
		filterOut : 
                    {op : "chain", filter : [
			{op : "map", fields : [
				{from : "PAGE_DOM", to : "SHARD"},
			]},
			{op : "field", from : "SHARD", 
			    filter : {op : "chain", filter : [{op : "hash"}, 
			        {op : "mod", mod : 64}, {op : "pad", left : "000"}]}},
		]},
	},

	output : {
		type : "file",
		path : ["{{DATE_YMD}}", "/", "{{DATE_HOUR}}", "/", "{{SHARD}}"],
		writer : {
			flags : {
				noAppend : true,
				compress : true,
			},
			factory : {dir : "split"},
			format : {
				type : "channel",
			},
		},
	},

Validating your Split Job
--------------------------
