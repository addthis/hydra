.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _sources:

##############
Data Sources
##############

In order to do anything interesting a process needs to have a data source to consume from.  Data sources come in all shapes and sizes and Hydra allows the job developer to consume from a large variety of data sources.  From flat files on local machine, binary encoded files in a cluster, or Kafka Brokers, Hydra makes it easy to consume from a variety of sources.  In a distributed system consumers of a single data source must coordinate in order to ensure that the entire source stream is consumed and that nothing is consumed more than once.  In the majority of cases the desire is to split the consumption responsibility amongst the consumers so that each source record is consumed by only one of the consumers.  In hydra we achieve this coordination by sharing a common configuration across all of the consumers.  Each consumer uses the same algorithm to determine which subset of the stream they should consume. 

Input Partitioning 
====================

There are two basic mechanisms Hydra users to partition the input stream.  The first is a *hash-based* partitioning scheme and the second is a *modulus-based* partitioning scheme.  If we define our hash partitioning function *f(x)* as the hash of the input file name mod the total number of consumers, *c*, then we can say that a consumer should only process a input file x if h(x) % c = i where i is the consumer number assigned to that consumer instance.  To make this a bit easier to understand lets look at an example.  Suppose the source stream is stored into files like:

1. file1
2. file2
3. file3

and we want to consume those files across two consumers.  Each consumer would apply the hash function to the three files and mod the result with 2 (the number of consumers).  The results may look like:

1. h(file1) % 2 = 0
2. h(file2) % 2 = 1
3. h(file3) % 2 = 0

If the first consumer is given the index 0 and the second consumer is given the index 1 then consumer 0 will process the first and third files while consumer 1 will process file2.  This approach yields the desired result in that each file is processed by one consumer.  There are some disadvantages to hash approach.  Generally source systems produce files with somewhat random file names.  The names do not often directly relate to the subset of data contained within them and the files can vary greatly when it comes to their size.  This deprives the job developer of insight into how the records are distributed amongst the consumers.  In practice this means that a individual consumers will get a random subsample of the source.  Random sub samples are fine in some cases but in other cases it can make the data analysis process more complicated and expensive.

To help address these issues Hydra also supports modulus based partitioning.  This type of source input partitioning requires that the files being consumed were generated with a known format that includes the modulus key in the file name.  When the files are generated the process generating the files will use some algorithm (typically a hash function applied to one or more keys of the record) to determine which file the record should be written to.  With the knowledge of how the records were paritioned and that the modulus id was included in the file name the network of consumers can partition the input files using the modulus ids directly.  This helps evenly balance the data across the consumers and gives the job developer knowledge about how the records are partioned.  This knowledge can be leveraged to make optimization assumptions later on (covered in later modules).  Lets look at an example.

Suppose we have three input files in the format ``{MOD}-000`` where ``{MOD}`` is the modulus for the given file

1. 001-000
2. 002-000
3. 003-000

Again we will asume two consumers, c0 and c1.  Now instead of applying a hash function to each of the file names they will split modulus id's amongst each other and then process any file that has a modulus they are responsible for.  In this case c0 will take partitions 1 and 3 and c1 will take partition 2.  The end result is the same as the hash-based partitioning in that each file is processed once and only once by a consumer but now the job designer has much more control and understanding about how the consumed data is distributed amongst the consumers.  

Source Types
=============

Hydra is a general purpose data processing system so it needs to be able to consume data from a wide variety of source types.  Hydra supports the following source types:

1.  :user-reference:`Local Files <com/addthis/hydra/task/source/DataSourceStreamFiles.html>` - consume files from the local or remote mounted file systems (great for testing)
2.  :user-reference:`Mesh Files <com/addthis/hydra/task/source/DataSourceMeshy2.html>` - consumes from Hydra's mesh network.  This is a distributed file system optimized for Hydra and the most common source type found in Hydra clusters
3.  :user-reference:`Aggregate <com/addthis/hydra/task/source/AggregateTaskDataSource.html>` - In some cases one source is not enough.  Aggregate data sources wrap multiple sources so that a single job can some data from multiple sources.

Source Formats
===============

A source can be broken up into two logical components.  The first component defines the *where* and the second component defines the *what*.  To successfully consume a data stream a process needs to know where (and how) it should consume the data and it needs to understand the format of the source files.  For example if the source is composed of rows of tab delimited data the process consuming the data needs to know what the value in the third column is.  Hydra currently understand the following formats:

1. :user-reference:`Channel <com/addthis/hydra/task/source/DataSourceChannel.html>` - accepts **codec** encoded data streams.  Codec is a binary encoding format specifically developed for Hydra and it is the most common format used within a cluster.
2. :user-reference:`Column <com/addthis/hydra/task/source/DataSourceColumnized.html>` - a row based format where each row is broken up into columns
3. :user-reference:`JSON <com/addthis/hydra/task/source/DataSourceJSON.html>` - consumes JSON objects

Compression
============

Disk space and network bandwidth are both precious commodities in a data analytics system.  Hydra sources atomically decompressed compressed data source on an as needed basis.  No custom configuration parameters are required to support decompression of compressed data source.  Hydra requires that the source file name indicates the compression type used.  Hydra supports the following compression algorithms:

1.  `gzip <http://www.gzip.org/>`_ - very good compression ratio but CPU intensive
2.  `lzf <https://github.com/ning/compress>`_ - compression algorithm optimized for speed
3.  `snappy <https://code.google.com/p/snappy/>`_ - a very fast compression algorithm created by google
4.  `bz2 <http://www.bzip.org/>`_ - good compression ratio (within 10% of optimal).  decompresses 6x faster than it compresses
5.  `lzma <http://en.wikipedia.org/wiki/LZMA>`_ - compresses better than bz2 and still has good decompression speed

Bottom line is that while its important to have a general understanding that Hydra is decompressing your source data automatically there is nothing for the job designer to manipulate when it comes to consuming compressed data sources.

Selecting Date Ranges
======================

By default Hydra expects source systems to segment their data by some date range.  Typically this is done by using a different directory for each date.  It can also be accomplished by storing the date in the file name itself.  The reason source data is segmented by date is to make processing a specific time period or a range of time periods more efficient.  Imagine a source that generates data and has data files that roll to a new file name each hour.  In one day it would generate 24 files, in one year it would generate 8760 files, and in ten years it would generate 87,600 files.  The volume of files generated would make the process of sifting through all of that data very expensive.  So source systems save their data files to directories using some date range granularity (e.g. day, week, month, etc).  When a source wants to consume the data from January 2nd it only needs to scan the files stored in the January second directory.

A typical source directory structure may look like:

.. code-block:: haskell

    /data
       /130201
       /130202
       /130203
       /130204
       /130205


Each directory is in the form YYMMdd.  Data for a specific day will be stored in the matching directory.  As a data consumer the source will request data from a specific date rather than the full range of data.  It is possible to use data directories with formats other than YYMMdd as the date format is a parameter specified in the source definition.

The job designer will specify the desired ranges.  In the case of static dates, e.g. process March 3rd through March 10th the dates can be provided in a format that matches the source's date format.  If the date ranges are dynamic, e.g. process any new data thats arrived in the last three days where the last three days moves forward as the actual calendar date format moves forward the user can define a macro for the start and end dates.  The macro to process new data for the previous three days looks like:

.. code-block:: javascript

    startDate:{{now-3}},
    endDate:{{now}}

Hydra will automatically replace ``{{now-3}}`` with todays date minus three in the format specified in the source.

Marking Progress
=================

Hydra sources keep track of the data they've already processed to prevent needless work.  A *MarkDB* is a database that is automatically generated by the Hydra job.  It is a simple key-value store where the key is the name of the file and the value includes:

1.  The number of bytes from that file that have already been processed
2.  A boolean flag that is set to true if all records from the file have been processed
3.  A date that represents the last modified time of the file

As the source is fed new files to process it searches the MarkDB for an existing entry for the given file.  If the file is not already in the database a new entry is created and the consumer will process that file.  If there is a entry for the file and the file was not completely processed during the previous run or the file was completely processed but the last modified time has changed the file will be processed starting from the first byte that has not already been processed.  If those conditions are not true than the source will skip that file because all of its data has already been consumed.  This is important because re-scanning every file every time a job runs would be hugely expensive.  

**Note**  Not all source types mark progress in the same way.  So it is not recommended to change the source type of an existing job.  That may lead to reprocessing data and incorrect results.

Examples
=========

In this section will look at several examples of source definitions for Hydra jobs.

Example 1: Consuming newline separated files from the local file system
------------------------------------------------------------------------

In this first example we will setup a simple source configuration for consuming files from the local machine.  The files will be stored in:

> /tmp/data

Each file will follow the naming convention:

> event_data.*

And within each file will be newline separated rows with three columns separated by a tab character:

> TIME UID URL

The following source configuration will consume 

.. code-block:: javascript

  {
    // set the type of this source to 'files'
    type:"files",
    // hash is set to true indicating that we want the consumers
    // to split the source files by hashing the file name
    hash:true,
    // read all data in the 'data' directory with a file name like event_data.*
    files:["/tmp/data/event_data.*"],
    // the factory tells the source how to interpret the consumed data
    factory:{
        // in this case our source data is set of rows with columnar data
        type:"column",
        // the factory needs to know how the rows are seperated
        // in this case each row is seperated by a newline character
        source:{
            type:"newline",
            // boiler plate, large majority of sources are injected
            source:{type:"inject"},
        },
        tokens:{
            // the default separator is a comma, we are overriding it with a tab
            separator:"\t",
        },
        // finally we tell the source how to define each column in the input stream
        columns:[
            "TIME",
            "UID",
            "URL",
        ],
    },
  }


Example 2: Consuming newline separated files from the Mesh filesystem
-----------------------------------------------------------------------

All Hydra clusters and many servers that provide data to a Hydra cluster participate in a mesh network.  This network acts as the backbone for routing data from machine to machine.  Each node in a mesh network is capable of relaying information from other nodes as well.  So as long as there is some path between two nodes (even if they are not directly connected) the system can transfer data between those nodes.

..
   http://en.wikipedia.org/wiki/File:NetworkTopology-Mesh.png placed in the public domain

.. image:: /_static/wikipedia-cache/NetworkTopology-Mesh.png

The servers participating in the mesh run a process that acts as the access point for that node.  The mesh process exposes a specified directory to peers in the network so all file references are relative to the base directory of the mesh process rather than the absolute file system path.

For this example we will consume the same file content as in the local file system example.  The only difference in terms of data structure is that the events will be stored on disk in a directory that identifies the date the events contained in the file were generated.

> stream/130404/event_data.*
> stream/130405/event_data.*

Note that in this case the base directory 'stream' is the directory where the mesh process is serving the data from.  The content of each file is the same as in the previous example, column separated values with new lines separating rows.

> TIME UID URL

The major difference between this example and the previous example is that because we are consuming from the mesh the consumers will consume files from any server connected to the mesh that has files matching our request.

The following configuration will consume the correct files from the mesh:

.. code-block:: javascript

  {
    // set the type of this source to 'mesh2'
    // 'mesh2' is the only mesh type you should ever use, legacy version
    // of mesh was superceded by this version
    type:"mesh2",
    // hash is set to true indicating that we want the consumers
    // to split the source files by hashing the file name
    hash:true,
    // the mesh configuration has multiple properties, so we use an
    // object to encapsulate the relevant parameters
    mesh:{
        // dates before this date will not be processed
        startDate:"{{now-3}}",
        // dates after this date will not be processed
        endDate:"{{now}}",
        // read all data in the 'data' directory with a file name like event_data.*
        // note that Hydra is a bit weird here and does not use standard date formatting
        // convetions.  'YYMD' is equivalent to 'YYMMdd' in java standard date formatting
        files:["{{YYMD}}/event_data.*"],
    },

    // the format tells the source how to interpret the consumed data
    // note that in some sources this section is called 'factory'.
    // 'format' is more descriptive of the content and new source types
    // use this convention.  Other than changing the header name the content
    // is exactly the same as the previous example
    format:{
        // in this case our source data is set of rows with columnar data
        type:"column",
        // the factory needs to know how the rows are seperated
        // in this case each row is seperated by a newline character
        source:{
            type:"newline",
            // boiler plate, large majority of sources are injected
            source:{type:"inject"},
        },
        tokens:{
            // the default separator is a comma, we are overriding it with a tab
            separator:"\t",
        },
        // finally we tell the source how to define each column in the input stream
        columns:[
            "TIME",
            "UID",
            "URL",
        ],
    },
  }


Example 3: Consuming files encoded using the codec format from the Mesh filesystem
-----------------------------------------------------------------------------------

So far we have consumed standard data files where new lines separate records and record values are separated by some delimiter.  These files are found every where (think apache access logs) and are nice because they are human readable.  They do have drawbacks and the main issue is that they are not space efficient.  The most common binary encoding used in Hydra systems is a custom format called *codec*.  *Codec* is sometimes referred to as *Bundle Formatted*.  Essentially this codec converts records into a binary representation that is space efficient.  

Another advantage of the bundle format is that the description of the data set is embedded within the bundle so consuming these files requires less custom configuration.  Taking the previous example and modifying it consume bundle formatted data rather than column formatted data.

The only change required is to the 'format' section.  The new format section will be:

.. code-block:: javascript

  format:{
      type:"channel",
      input:{type:"inject"},
  },


The full example now looks like:

.. code-block:: javascript

  {
    // set the type of this source to 'mesh2'
    // 'mesh2' is the only mesh type you should ever use, legacy version
    // of mesh was superceded by this version
    type:"mesh2",
    // hash is set to true indicating that we want the consumers
    // to split the source files by hashing the file name
    hash:true,
    // the mesh configuration has multiple properties, so we use an
    // object to encapsulate the relevant parameters
    mesh:{
        // dates before this date will not be processed
        startDate:"{{now-3}}",
        // dates after this date will not be processed
        endDate:"{{now}}",
        // read all data in the 'data' directory with a file name like event_data.*
        // note that Hydra is a bit weird here and does not use standard date formatting
        // convetions.  'YYMD' is equivalent to 'YYMMdd' in java standard date formatting
        files:["{{YYMD}}/event_data.*"],
    },

    // bundle formats are self describing so the format is simple
    format:{
        type:"channel",
        input:{type:"inject"},
    },
  }

Example 4: Consuming files encoded using the codec format from the a Kafka broker(s)
--------------------------------------------------------------------------------------

Hydra makes it easy to consume data from a `Kafka <http://kafka.apache.org/>`_ broker(s).  Hydra works with the current release version of Kafka 0.7x and there are no current plans to support the 0.8 version of Kafka that is in development.  Kafka stores data on the file system and breaks up data by *topic* and *partition*.  A Kafka *broker* is a process that producers can push data to and consumers can pull data from.  In order to fully consume a data stream from Kafka the consuming system must consume all partitions for the given topic from all brokers that have data for this topic.  Kafka uses `ZooKeeper <http://zookeeper.apache.org/>`_ to store information about brokers and topics.  Hydra connects with ZooKeeper when a job using a Kafka source starts to determine the complete list of broker/topic/partition triples that need to be consumed.  Hydra then allocates a subset of the broker/topic/partitions to each Hydra consumer node participating in the job.  It is important to note that Hydra does not add any intelligence to the partitioning logic so the consumers will get a random distribution of data as matching how it was stored in Kafka originally.  

Kafka uses byte offsets to mark the start of each message (record) it manages.  The first time a Hydra job consuming from a Kafka data stream starts it will find the byte offset for the message closest to the requested start date.  On subsequent runs of the job it will remember the byte offset of the last message processed and start processing from that point.

Here is an example of a Kafka source configuration:

.. code-block:: javascript

  source:{
    // indicate that this is a kafka data source
    type:"kafka",
    // must identify the name of the topic we'd like to consume
    topic:someDataTopic,
    // this is the directory where state information about what
    // data from each broker/parittion pair has been consumed
    markDir:"marks",
    // need to tell Hydra which ZK server/path to use to find information
    // about the topic we are consuming from
    zookeeper:"www.yourzookeeper.com/ZKPrefix",
    // Hydra will expand this section based on results from ZooKeeper to include the
    // full list of broker/partition pairs
    injectedBrokerList:%(injectedBrokers www.yourzookeeper.com/ZKPrefix marks someDataTopic)%,
    // the start date is only used the very first time the job runs
    // after that it will always consume the next available message from where
    // it left off the last time it was run
    startDate:"{{now-20}}",
  },


Example 5: Consuming from an Aggregate Data Source
---------------------------------------------------

An Aggregate data source is simply a wrapper around a list of data sources.  This is convenient when a single job needs to consume data from multiple sources.  In this example we will combine two of our previous examples into a single aggregate source. 

The most important thing to note is that when using a aggregate data source you must override the *markDir* for each source in the aggregate list.  Recall that the markDir is used to store information about what data has already been consumed from a given source.  If two sources use the same (default) markDir unexpected behavior will occur due to conflicts between the two sources.

.. code-block:: javascript

  source:{
    type:"aggregate",
    sources:[
        {
            // indicate that this is a kafka data source
            type:"kafka",
            // must identify the name of the topic we'd like to consume
            topic:someDataTopic,
            // this is the directory where state information about what
            // data from each broker/parittion pair has been consumed
            markDir:"kafkaMarks",
            // need to tell Hydra which ZK server/path to use to find information
            // about the topic we are consuming from
            zookeeper:"www.yourzookeeper.com/ZKPrefix",
            // Hydra will expand this section based on results from ZooKeeper to include the
            // full list of broker/partition pairs
            injectedBrokerList:%(injectedBrokers www.yourzookeeper.com/ZKPrefix marks someDataTopic)%,
            // the start date is only used the very first time the job runs
            // after that it will always consume the next available message from where
            // it left off the last time it was run
            startDate:"{{now-20}}",
        },
        {
            // set the type of this source to 'mesh2'
            // 'mesh2' is the only mesh type you should ever use, legacy version
            // of mesh was superceded by this version
            type:"mesh2",
            // hash is set to true indicating that we want the consumers
            // to split the source files by hashing the file name
            hash:true,
            // override the markDir so it does not conflict with the kafka source
            // in the aggregate source list
            markDir:"meshMarks",
            // the mesh configuration has multiple properties, so we use an
            // object to encapsulate the relevant parameters
            mesh:{
                // dates before this date will not be processed
                startDate:"{{now-3}}",
                // dates after this date will not be processed
                endDate:"{{now}}",
                // read all data in the 'data' directory with a file name like event_data.*
                // note that Hydra is a bit weird here and does not use standard date formatting
                // convetions.  'YYMD' is equivalent to 'YYMMdd' in java standard date formatting
                files:["{{YYMD}}/event_data.*"],
            },

            // bundle formats are self describing so the format is simple
            format:{
                type:"channel",
                input:{type:"inject"},
            },
        }
    ]
  },


Example 6 - Sorted Inputs
--------------------------

.. code-block:: javascript

  {
    type:"sorted",
    // define how the bundles should be sorted
    comparator:{
        // field array can have 1 to n bundle fields to
        // include in the comparison.  the code makes
        // and attempt to intelligently sort the fields
        // for examples ints ans floats will be compared as ints and floats,
        // strings will be compared as strings.  But this
        // class does not currently support fields of Array type
        field:["field1", "filed2",...,"fieldn"],
        ascending:true,
    },
    // the number of elements to batch before sorting
    elements:100,
    source:{
        type:"mesh2",
        shardTotal:%[shard-total:32]%,
        mesh:{
            meshPort:"%[meshPort:5000]%",
            startDate:"%[start-date:{{now-2}}]%",
            endDate:"%[end-date:{{now}}]%",
            sortToken:"/",
            sortTokenOffset:6,
            dateFormat:"YYMMddHH",
            files:["/job/%[job]%/*/gold/split/{Y}{M}{D}/{H}/{{mod}}-*"],
        },
        format:{
            type:"channel",
            input:{type:"inject"},
        },
    },
  }


Exercises
==========

Exercise 1
-----------

Modify the example from Example 1 to consume gzip compressed files.

Exercise 2
-----------

Modify the example from Example 2 to consume comma separated rather than tab separated data.

Exercise 3
-----------

Suppose you have data on a server with a mesh process.  The data we want to consume is stored using bundles.  The following list shows the example path (relative to the base directory of the mesh process) and file names for the files we want to consume.  

1.  datadir/13/03/25/datafile-1.gz
2.  datadir/13/03/25/datafile-2.gz
3.  datadir/13/03/26/datafile-1.gz
4.  datadir/13/03/27/datafile-1.gz

The data is stored on 15 servers.  Create a source to consume the data.
