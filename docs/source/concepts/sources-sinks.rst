.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _sources-sinks:

#######################
Data: Sources and Sinks
#######################

Jobs can in principle be arbitrary programs [#]_.  In practice most jobs ingest streams of data that is transformed into :ref:`bundles <bundles-values>`, applies some :ref:`filters <filters>` to those bundles and emits an output.

A rough skeleton might look like:

.. code-block:: javascript

    {
      type:"map",

      source:{
        type:"mesh2",
        // Where to pull files
      },

      map:{
        fields:[
          // Which fields to process
          {from:"TIME", to:"TIME"},
          {from:"METHOD"},
        ],
        filterOut:{op:"chain", failStop:false, debug:true, filter:[
          {op:"time", src:{field:"TIME", format:"yyyy-MM-dd", timeZone:"GMT"}, dst:{field:"TIME", format:"native"}},
        ]},
        // And some other stuff to choose the SHARD and DATE_YMD
      },

      // In this case out putput is just flat files
      output:{
        type:"file",
        path:["{{DATE_YMD}}","/","{{SHARD}}"],
        writer:{
          factory:{dir:"split"},
          format:{
            type:"column",
            columns:["TIME", "METHOD"]
          },
        },
      },
    }

``mesh2`` is a source for pulling files.  The source could be changed to pull from `Apache Kafka`_ brokers without changing the output.  Or if the output could be changed to hydra trees without changes to the source configuration. Put another way the *format* of incoming data matters, but where it comes from and how it gets there is abstracted away.

.. _Apache Kafka: http://kafka.apache.org/

.. [#] For example ``Hoover`` rsyncs raw files into the cluster.

Sources
########

Sources (formally ``TaskDataSource``) support turning incoming data into streams of ``Bundles`` with a few simple operations:
 * ``next()``
 * ``peak()``
 * ``close()``

The dual role of "get bytes from somewhere" and  "make these bytes a bundle" spread among modular classes that can be combined together.  So there are classes focused on format:
 * ``channel`` (aka already self describing bundles)
 * ``kv.txt``
 * ``column``
 * ``json``
 * ``columnar``

And ones focosed on where the bytes come from:
 * ``files``
 * ``streamv2`` (Streamserver)
 * ``mesh2``

So a ``streamv2`` source might defer to a ``column`` or ``json`` parser.  Multiple sources can also be combined together with ``aggregate``.  A job might pull from two different kafka topics, or consume serveral different types of access logs to a common format.


Who's on first?
-----------------

Broadly, sources are either the output of hydra splits and have consistent :ref:`shards <shards-pipelines>` or we do a hash on the file name.  When you see referces to shards or hashes in the source it's dealing with which data the 7th of 9 nodes get.


Sinks
######

Output options include:
 * Flat files
   - `Columnar`
   - channel
   - column
 * :ref:`Hydra Trees <paths-trees>` 
 * `Cassandra`_

.. _Cassandra: http://cassandra.apache.org
.. _Columnar:

Columnar flat files take advantage of columnar compression while still providing row-based APIs.  Columnar compression
allows the job designer to provide *hints* to the compression algorithms which can then optimally compress the data.
Compression reduces the number of bytes required to represent the data on disk.  Fewer bytes means less I/O and less I/O
means better disk and network performance.  There are currently five column types supported by this output data type:

 * RAW: compresses data using a the native compression scheme for the provided value
 * TEXT255: creates an index map with the most common 255 column values per block
 * DELTAINT: stores variable length integers using delta encoding
 * DELTALONG: stores variable length longs using delta encoding
 * RUNLENGTH: stores the number of consecutive values of the same value


Just as sources can be thought of "where to get data" and "how to make bundles", outputs  transform bundles back into bytes, and put them somewhere.  Using the same example snippet as before:

.. code-block:: javascript

    output:{
        type:"file",
        path:["{{DATE_YMD}}","/","{{SHARD}}"],
        writer:{
            factory:{dir:"split"},
            format:{
                type:"column",
                columns:["TIME", "METHOD"]
            },
        },
    },

The ``type``, ``path``, and ``factory`` determine where the bytes should be written.  In this case in local files like ``split/120512/000-000.gz``.  The format of those files is delimited (think csv or tabs) columns, with only two boring columns from an HTTP access log.
