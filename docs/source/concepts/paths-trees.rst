.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _paths-trees:

###################
Paths through Trees
###################


The Programmer as Navigator
###########################

  There is a growing feeling that data processing people would benefit if they were to accept a radically new point of view, one that would liberate the application programmer's thinking from the centralism of core storage and allow him the freedom to act as a navigator within a database.  To do this, he must first learn the various navigational skills; then he must learn the "rules of the road" to avoid conflict with other programmers as they jointly navigate the database information space.
  This reorientation will cause as much anguish among programmers as the heliocentric theory did among ancient astronomers and theologians.

  Charles W. Bachman. 1973 Turing Award Lecture. [navigator]_ 

Hierarchical databases adopt a view of looking at databases that was centered on humans instead of what was convenient for computers.  Furthermore they allow focus on exploring that data that is there (descending through a tree, turn right, turn left, go back up) instead of working backwards from constraints expressed in a schema.  Hydra is premised on the view that this exploration centric view is the right way to start looking at new data source.

There is a flip side though.  Once a round of exploration is complete (perhaps using a series of trees as views into the data) a new tree must be devised that provides reasonable performance tradeoffs for whatever repeatable queries are desired of it.  You might call this "queries, second" thinking and is akin to the query first thought process used in column oriented databases and complex stream processing.

We think this ease of initial exploration, followed by explicit query centric thinking is the right workflow to encourage.  In particular we argue that explicit query based performance optimization is preferable to after the fact re-organization of a data-first schema.


.. [navigator] The programmer as navigator. Commun. ACM 16, 11 (November 1973), 653-658. DOI=10.1145/355611.362534 http://doi.acm.org/10.1145/355611.362534


You already know about trees
------------------------------

Tree based data structures are already well known from any introductory computer science course.  File systems and URI paths are among the most common examples.

There is also an advantage for distributed systems. It is notoriously difficult to provide threads concurrent access to in memory trees.  But they can be intuitively partitioned, split, and merged  (adding the counts for two a node on two physical hosts together really is just addition: associative and commutative). Contrast to guessing which part of a normalized relational database might be the shard key.



Paths
#####

To create a tree from a data source it must first be described.  Consider an incoming stream of high level product metrics.  These metrics are also broken down by dimension (perhaps we want to compare conversions for users given two different versions of a web page).

Since to be useful we would also probably break things down by time, we might get a path like this:

.. code-block:: javascript
  :linenos:

   paths:{
      PRODUCT:[
        // date/product/metric/dimension
        {type:"const", value:"product-metrics"},
        {type:"const", value:"ymd"},
        {type:"value", key:"DATE_DAY"},
        {type:"value", key:"PRODUCT_CODE"},
        {type:"value", key:"METRIC"},
        {type:"value", key:"DIMENSION_NAME"},
        {type:"value", key:"DIMENSION_VALUE"},
      ],
    },

It's a common convention to have some constant at the root of the tree so it's always clear what you are looking at. Each line under the ``PRODUCT`` path is called a path element. The ``PRODUCT`` path contains seven path elements.  The ``const`` path element specifies a single node in the tree that stores a specific value. We have declared two ``const`` path elements to help label the tree. The ``value`` path element specifies one or more nodes. The number of nodes created is equal to the number of unique elements that are stored in the bundle field that is specified with the 'key' parameter. A sibling node is created for each unique element of the bundle field.

In the previous example the root of the tree is a node with the value ``product-metrics``. The root node has a single child with the value ``ymd``. The node with the value ``ymd`` has N children nodes, one node for each day that is processed. Each day node has M children nodes, one node for each product that is processed. Similar subtrees exist for the metrics, dimension names, and dimension values.

A very common pattern is to specify parent and child path elements where the parent is a ``const`` path element with a string value to label the tree. Lines 5 and 6 of the previous example use this pattern. It is possible to implicitly declare the parent ``const`` path element by specifying the ``name`` parameter to the child path element. In computer science this is known as `syntactic sugar`_. The following path is equivalent to the previous example. How lines 5 and 6 from the previous example have been combined in line 5 of the following example.

.. _syntactic sugar: http://en.wikipedia.org/wiki/Syntactic_sugar

.. code-block:: javascript
   :linenos:

   paths:{
      PRODUCT:[
        // date/product/metric/dimension
        {type:"const", value:"product-metrics"},
        {type:"value", name:"ymd", key:"DATE_DAY"},
        {type:"value", key:"PRODUCT_CODE"},
        {type:"value", key:"METRIC"},
        {type:"value", key:"DIMENSION_NAME"},
        {type:"value", key:"DIMENSION_VALUE"},
      ],
    },



A contrived cat camera product might emit a stream of data that looked something like this::

  $ cat cats.txt 
  110927,CatCam,views,captions,2,
  110928,CatCam,views,bytes,10698,
  110928,CatCam,views,captions,3,
  110928,CatCam,views,captions,2,
  110928,CatCam,views,bytes,90622,


The ``PRODUCT`` path could then be applied to generate a tree summarizing the cute kitten information. ::

  * product-metrics:5
   * ymd:5
    * 110927:1
     * views:1
      * captions:1
       * 2:1
    * 110928:4
     * views:4
      * captions:2
       * 2:2
       * 3:1
      * bytes:2
       * 3:1
       * 3:1

The number after the ``:`` is the count or hits of that node.  So for example there were 4 hits on the day  ``110928``. This can be somewhat confusing if the node key is a number.  If you needed to keep track of several secondary counts (such as the number of requests, and number of bytes for a particular URL) you might want to use a ``sum`` :ref:`data attachment <data-attachments>`.
