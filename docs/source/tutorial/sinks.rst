.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _sinks:

###########
Data Sinks
###########

In computing the term *sink* refers to a process that receives incoming events.  In Hydra's case the incoming events are the events consumed from a source.  The Hydra data sync is responsible receiving these events and emitting them to the local file system.  There are several types of sinks in Hydra:

1. :user-reference:`File <com/addthis/hydra/task/output/DataOutputFile.html>` - a sink that emits events to flat files
2. :user-reference:`Tree <com/addthis/hydra/task/output/tree/TreeMapper.html>` - a sink that emits events to a tree (also called a map) data structure.  
3. :user-reference:`Chain <com/addthis/hydra/task/output/TaskDataOutputChain.html>` - a sink that wraps multiple outputs
4. Cassandra - a sink that emits records to a `Apache Cassandra <http://cassandra.apache.org/>`_ cluster.

File Sinks
============

File Sinks are used in what are commonly referred to as *Split Jobs*.  A Split Job consumes events and emits the data to the local file system in some record based format. 

Motivations for using File Sinks
----------------------------------

One reason to ingest data into a hydra cluster is that the source system may just be a single server and having a large number of cluster nodes consume data concurrently from a single (or small number) of sources may overwhelm those servers.  Ingesting the data into the cluster spreads the data across a a larger number of servers allowing an even a larger number of servers to consume the data from those servers. 

Another reason data is ingested into a cluster is for long term storage.  One of the functions Hydra serves is that of a distributed file system.  With hundreds of servers participating in a cluster the net storage capacity can be very large.  Rather than relying on a SAN for long term storage data can be consumed from sources with limited capacity and stored inside of the cluster for long term storage.

In large data systems it is often useful to store the same data sets partitioned by different functions.  For example it may be desirable to store event data partitioned by user ID and by URL.  This way downstream jobs can consume the data from the data source with the ideal partitioning scheme for the given use case. 

Data cleansing is yet another reason people build Hydra Split jobs.  In this use case filters may be applied to the data to remove uninteresting data (making downstream jobs faster), remove *bad* records, and/or add additional fields to the records (e.g. combining two or more fields to create a new value).  

If the source data is stored in an inefficient or undesirable file format (for one reason or another) a File Sink may be used to convert the source data into an alternative data format.  For example if the source data is in a new line separated column delimited format which is not space efficient a file sink can consume that data and write it using a binary encoding scheme.

File Sink Configuration
=========================

A file sink's configuration is fairly simple.  Essentially the sink needs to be told where to write the data and how it should write it.  The sink receives the events (records) consumed by the task's source so it has full access to the values contained in those events.  These values are used to template the output.  The two sections the job author needs to configure are the *path* and *writer*.

Output Path
------------

The output path is a path relative to the base directory of the hydra process.  As a job designer you want to create a path that will optimize data consumption for downstream jobs.  Typically this involves splitting the data into time buckets (typically days) and including a modulus identifier in the file name.  This allows downstream jobs to consume a specific time range and to partition the source data using the modulus method (desribed in detail in Module 1).  A typical path element looks like:

.. code-block:: javascript

  path:["{{DATE_YMD}}","/","{{SHARD}}"],


The elements inside of the double curly braces are fields extracted from the event sent to the sink.  So in this case we expect a field identified by the key 'DATE_YMD' and another field identified by 'SHARD'.  The path is broken into a list of elements.  The elements may be variable or string constants.  The system will concatenate the values into single string.  The example above will turn into an output like:

> 120423/002-000

The SHARD variable in the example is above is '002', Hydra adds the '-000' to the file to represent the version number of the file.  There are instances when the sink can not append to a file and in those cases it will generate a new version of the '0002' shard, e.g. '002-001'.  The naming scheme limits us to 999 versions of each SHARD.  In practice this limit is not an issue.

Data Channel Writers
----------------------

After the path the next thing to configure is the output writer.  In Hydra they are called DataChannelWriter(s). The DataChannelWriter tells Hydra how the data should be written.  This section describes configuration parameters available to Hydra job developers to control how data is written to output files.  

1. :user-reference:`Output Flags <com/addthis/hydra/task/output/OutputStreamFlags.html>`
    a. noAppend - a boolean indicating if data can be appended to an existing file.  Defaults to false.
    b. maxSize - the maximum size (human readable) that a file can grow to before rolling over to a new file
    c. compress - if true the data will be written to a compressed data stream
    d. compressType - allows job creator to specify compression format 
    e. header - a string to include at the top of each file
2. :user-reference:`Output Factory <com/addthis/hydra/task/output/OutputWrapperFactory.html>`
    a. dir - the root directory to store the output files in.  Note this is relative to the working directory of the host process
    b. multiplex - a boolean value indicating that the Muxy file system should be used
3. Output Format
    a. column - a human readable column format.  Job designer specifies which fields to include in the output
    b. channel - a file encoded using the binary Codec format.  Job designer specifies which fields to *exclude* from the output
4. maxOpen - the number of open files the process may have open at any given time.  Set the number too high and the file system may break because it can't handle a very large number of open files.  If the value is set too low then the process will have to open and close files rapidly (thrashing).  A reasonable default is 1024.
5. filter - A BundleFilter that can be used to filter and manipulate a record before it is persisted to the output stream

Purging Data
-------------

Although it would be great to keep all data for all time that is not always possible or desirable.  Hydra includes a simple purging system for deleting files stored in directories or files that include the date.  Essentially you can instruct hydra to delete data older than a certain number of days or hours.  The date may be in the directory structure or in the file name.

This section is optional.  If it is not included then the data associated with this job will grow unbounded.

Examples
==========

Here are a few examples of Hydra File output configurations:

Example 1
----------

In this first example we look at a simple file output that stores data using Hydra's binary codec.  Each file is limited to 64 megs in size and the files are stored compressed.  Since the configuration does not specify a compression type the default gzip compression algorithm is used. 

Since the output uses *channel* (Hydra's binary codec) the default is to include all fields in the incoming record in the output.  To execlude fields the configuration needs to specify which fields to include. 

The configuration sets *maxOpen* to 500.  This means that at any given moment the process can have a maximum of 5000 open files.  Any more than that and it will start closing files and then re-opening them if new data for those files is found.  This job configuration does not set the *noAppend* flag which means the files that are closed may be re-opened and appended to.  If that flag were set the number of files would rise dramatically with thrashing because each time a file is closed and can never have new data appended to it.

As a job designer you need to be able to think about how many files your process will be generating to determine how many files will likely be open at any given moment.  In this case the *path* variable is storing data by date (year/month/day) and *SHARD*.  The *SHARD* is a common variable name used in Hydra jobs to represent the partition number of the output record should be assigned to.  So if the input data is for one day and there are 128 possible partitions this process would have 128 files open at any given time.  If the number of partitions were larger or the range of days of processed was expanded then the number of files would grow using the simple formula:  # of days * # of partitions. 

.. code-block:: javascript

  output:{
    type:"file",
    path:["{{DATE_YMD}}","/","{{SHARD}}"],
    writer:{
        maxOpen:500,
        flags:{maxSize:"64M", compress:true},
        factory:{dir:"split"},
        format:{
            type:"channel",
            exclude:["FIELD_1","FIELD_4","FIELD_7"],
        },
    },
  },

Example 2
-----------

In this example we'll modify the output to use the multiplexed file manager.  The multipliexed file manager is described in detail in Module X.  At a high level it provides an abstraction for the file system giving the application a virtual file system that more effiecently handles a very large number of files.  The OS file system will store a small number of meta data and data files related to the multiplexed file manager and the APIs provided by the client allow the application to work with the virtual files in the same way would if the files are actually stored on disk.  The key point is that although the physical storage on disk changes the application level logic remains the same.  

The motivation for this is that some jobs can generate many 100s of thousands of files and this is hard for most file systems to deal with.  The configuration change to enable this feature is one additional parameter to the *factory* component of the writer.  **NOTE** you cannot change from standard to multiplex or multiple to standard once a job has already starting processing data.  

.. code-block:: javascript

  output:{
    type:"file",
    path:["{{DATE_YMD}}","/","{{SHARD}}"],
    writer:{
        maxOpen:500,
        flags:{maxSize:"64M", compress:true},
        factory:{dir:"split", multiplex:true},
        format:{
            type:"channel",
            exclude:["FIELD_1","FIELD_4","FIELD_7"],
        },
    },
  },


Example 3
----------

This example switches from the binary codec to a column based output.  The only change from Example 2 is in the format section.  This means we are still using the multiplexed output file format but the data stored in that format will be column separated records rather than binary codec information.

.. code-block:: javascript

  output:{
    type:"file",
    path:["{{DATE_YMD}}","/","{{SHARD}}"],
    writer:{
        maxOpen:500,
        flags:{maxSize:"64M", compress:true},
        factory:{dir:"split", multiplex:true},
        format:{
            type:"column",
            columns:["FIELD_2","FIELD_3","FIELD_4","FIELD_5","FIELD_6"],
        },
    },
  },


Map (Tree) Sinks
==================

A Hydra *Map* Job is a job that outputs a tree data structure.  Map jobs convert input in the for of records into a tree.  Data is extracted from the Tree using the Hydra Query system to convert the data back into rows. 

.. image:: /_static/hydra-in-row-tree-out-row.png

Tree data structures are well suited for data aggregation use cases.  Their structure naturally compresses the input data sets.  The easiest way to understand this is to look at an example.  

**Input**

ID   URL                         DATE
--   --------------------------  --------
1    www.foo.com                 13/01/01
2    www.bar.com                 13/02/01
3    www.foobar.com              13/03/01

Imagine that instead of 3 input records we have 300M input records and we'd like to know the number of visits to each unique Domain and we would like that data broken down by day.  In a traditional RDBMS data model we may create a table to store the event records and then use SQL to get the required information.  In the RDBMS model the URL value would be repeated in the data store for each event.  The RDBMS would likely use a b-tree index on the date field to enable range queries.  A Hydra tree would define a tree like the following:

.. image:: /_static/hydra-sample-root-url-date.png

Each unique Domain would be persisted only once in the tree.  The date nodes are children of the Domains so they would be repeated for each domain that had events on the same date.  We could have designed the tree with URLs as children of dates but then each unique URL would be repeated for each day the URL was visited.  URLs require more storage than a date string (in general) so we decided to repeat Dates rather than domains.  As events are consumed by the job the tree will be updated with the information required.  If the tree pat representing the data record already exists than the meta data on that path will be updated.  For example a counter will be incremented that can be used later to determine the number of records that matched a given path.  If the path does not exist than new nodes will be created to represent it.

Thus far we've talked about a Hydra tree as if it was a single entity.  In Hydra there exists one tree for each processing node consuming data.  In the example above to get the total number of visits to domain www.foo.com on January 1st the system needs to sum the results from each tree on each processing node.  This is an example where understanding how your data is partitioned can be helpful.  If the data is partitioned by domain, meaning that all data for a given domain goes to a single partition, then we can answer the question without aggregating the data across computational nodes.

**INSERT IMAGE HERE**

Using the same input as was used in the File Sink example above we will build a Hydra tree matching the same tree structure as depicted above.

.. code-block:: javascript

  output:{
      type:"tree",
      root:{
          {type:"const", value:"root"},
          {type:"value", key:"DOMAIN"},
          {type:"value", key:"DATE_YMD"},
      }
  }

Notice that the type of the output is now *tree*.  In the file examples above it was *file*.  This tells Hydra to build the tree data structure using the configuration provided by the job designer.  In this case the tree is very simple with a constant value for the node at the root of the tree followed by the values of DOMAIN and DATE_YMD from the input record.  As each new record is added Hydra analyzes the tree structure (in order) to determine what action to take.  In hydra the parent/child relationship is inferred from the order of the configuration.  In the example above root is the parent the domain values and domain value is the parent of the unique DATE_YMD values.  When the first record is consumed the root node does not yet exist so it will be created.  The value of DOMAIN will be added as a child node to the root node and the value of DATE_YMD will be added as a child to the DOMAIN node.  If the node already exists in the tree, for example we've already seen www.foo.com, then the meta data associated with that node will be updated to reflect a new *hit* on that node.

To illustrate this process we will build a tree using the following input data.  In the tree diagram the nodes will be appended with colon and a number.  That represents the number of *hits* for that particular node.

ID   URL                         DATE
--   --------------------------  --------
1    www.foo.com                 13/01/01
2    www.bar.com                 13/02/01
3    www.foo.com                 13/03/01

- As the first record is processed the root node will be created


.. code-block:: text

  ->root:1


- www.foo.com does not yet exist as a child of root so that node will be added

.. code-block:: text

  ->root:1
    ->www.foo.com:1


- Next date, in YYMMdd format is added as a child of www.foo.com

.. code-block:: text

  ->root:1
   ->www.foo.com:1
    ->130101:1


- The next record is processed.  The root node already exists so Hydra will just update the hit count for that node

.. code-block:: text

  ->root:2
   ->www.foo.com:1
    ->130101:1


- The domain in the second record has not been created yet so Hydra will create it now:


.. code-block:: text

  ->root:2
   ->www.foo.com:1
    ->130101:1
   ->www.bar.com:1


- And the date for that event will be added as a child of the www.bar.com node


.. code-block:: text

  ->root:2
   ->www.foo.com:1
    ->130101:1
   ->www.bar.com:1
    ->130201:1

- When the last record is processed the root node and the domain node already exist.  Their hit counts will be updated

.. code-block:: text

->root:3
 ->www.foo.com:2
  ->130101:1
 ->www.bar.com:1
  ->130201:1


- And the new date node will be added as a child of www.foo.com


.. code-block:: text

  ->root:2
   ->www.foo.com:2
    ->130101:1
    ->130301:1
   ->www.bar.com:1
    ->130201:1


Hydra Map jobs are extremely flexible and feature rich.  More complex examples will be covered in the **INSERT MODULE HERE**.

Cassandra Sinks
=================

Both *File Sinks* and *Map Sinks* emit data to the local file system of the processing node.  The Cassandra Sink is different in that the Sink sends data to a remote database, Cassandra in this case.  When needed Hydra's Cassandra output type makes writing data to a C* cluster straight forward.

.. code-block:: javascript

  {
    type:"cassandra",
    cluster:{
        clusterName: "cassandra cluster name",
        hosts:['chost1','chost2','chost3'],
        autoDiscoverHosts:true,
        runAutoDiscoveryAtStartup:true,
    },
    keyspace:{
        name:"yourkeyspace",
    },
    columnFamilies:[
        {
            dangerOverrideExisting:false,
            name:"ColumnFamily1",
            comparatorType: "UTF8Type",
            compactionStrategyOptions: {"sstable_size_in_mb":"10"},
            compressionOptions: {"sstable_compression": "SnappyCompressor", "chunk_length_kb":"64"},
        },
        {
            dangerOverrideExisting:false,
            name:"ColumnFamily2",
            comparatorType: "UTF8Type",
            compactionStrategyOptions: {"sstable_size_in_mb":"10"},
            compressionOptions: {"sstable_compression": "SnappyCompressor", "chunk_length_kb":"64"},
        },
    ],
    mutations:[
        {columnFamily:"ColumnFamily1", rowKey:"ROW_KEY1", columns:[
            {type:"string-string", constKey:"c1", cvalue:"SOME_VALUE", ttl:"2592000", allowEmptyValues:false},
            {type:"string-string", constKey:"c2", cvalue:"OTHER_VALUE", ttl:"2592000", allowEmptyValues:false},
        ]},
        {columnFamily:"ColumnFamily1", rowKey:"ROW_KEY2", columns:[
            {type:"string-string", constKey:"c1", cvalue:"FOO_VALUE", ttl:"2592000", allowEmptyValues:false},
            {type:"string-string", constKey:"c2", cvalue:"BAR_VALUE", ttl:"2592000", allowEmptyValues:false},
        ]},
    ],
    sink:{
        mutationThreads:4,
        maxQueueSize:500,
    },
  },


Exercises
============

Exercise 1
------------

Modify example 1 so that the data is stored uncompressed and uses a column format rather than the channel format.

Exercise 2
-----------

Modify the Hydra Map job example so that the Date nodes are the parents of the Domain nodes.

Exercise 3 (advanced)
----------------------

Imagine that the input to the Hydra Map job example had a forth column storing the country the event is associated with.  Modify the tree so that it has the following structure:

.. code-block:: text

 ->root
  ->all
   ->DOMAIN
   ->DATE_YMD
  ->bycountry
   ->COUNTRY
   ->DOMAIN
   ->DATE_YMD

