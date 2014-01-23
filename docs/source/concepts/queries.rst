.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _queries:

########
Queries
########

So now that we have these spiffy trees running on distributed systems, how can any information be extracted from them?  Hydra uses a terse query language to express transformations of trees to columnar output.  Queries have two parts:
 * an expression that describes what part of the path to capture
 * a set of operations to perform


Paths
#####

Recall our simple product tracking :ref:`sample <paths-trees>`.  If we wanted to get how many total events per day were received we would use a path like::

  product-tracking/ymd/+:+hits

Which would return a two columns: the date, and number of hits per day. 

Descent through the tree uses slash delimited keys just like a file system.  ``:`` is used to get at node elements while ``+`` is used to capture the elements at that level.  A more complicated (but contrived) example::

  product-tracking/ymd/*:hits>1/++foo:+hits

captures metrics start start with "foo" on days with more than one event received (of any type), and includes the number of hits.

Let's define the difference between **matching** a node and **collecting** a node. *Matching*
against a set of nodes allows the path traversal to continue into the children of
the nodes. When a node is matched against it is not included in the output results.
In the previous example the set {"product-tracking"} is matched against at the top level,
the set {"ymd"} is matched against in the second level, and the set of all nodes
is matched at the third level using the "*" notation. *Collecting* a set of
nodes continues the traversal into the children and includes the nodes in
the output. In the previous example the set of all nodes that begin with the prefix
"foo" the fourth level are collected using the "++" notation. The "+" character is used
at the beginning of a node declaration to specify a collection operation.

Path Specification
------------------

The grammar for the path specification is below. You can ignore this grammar specification if
you are unfamiliar with context-free grammars. We will be covering in detail each of the components
of the grammar.

.. productionlist::
   QueryPath: "/"? QueryElement ("/" QueryElement)* "/"?
   QueryElement: QEPrefix? QENode
   QEPrefix: QELimit | QESkipLimit | QEAccept
   QELimit: "(" number ")"
   QESkipLimit: "(" number "-" number ")"
   QEAccept: "~"
   QENode: QENReserved | ( QENRegex? QENComponentList (QEPropertyList | QEField)* )
   QENReserved: "+" | "*" | ".." | "+.."
   QENRegex: "|" | "!"
   QENComponentList: QENSimpleList | QENFlatList
   QENSimpleList: ","? QEComponent ("," QEComponent)*
   QENFlatList: ";"? QEComponent (";" QEComponent)*
   QENComponent: QENPrefix? (QENNode | QENDataAttachment)
   QENPrefix:  "+" | "++" | "+{" number "}"
   QENNode: string
   QENDataAttachment: "%" string (= string)*
   QEPropertyList: ":" QEProperty ("," QEProperty)*
   QEProperty: "+"? BoundedValue
   QEField: "$" "+"? string ("=" LtGtValue)?
   BoundedValue: string BoundedValueLimit*
   BoundedValueLimit: ("<" | "=" | ">") num
   LtGtValue: string LtGtValueLimit*
   LtGtValueLimit: ("<" | ">") num


Query Paths (QueryPath)
   A path is a sequence of elements that are separated by forward-slashes ("/" characters)

   ::

       /a/b/c/d             A path with four elements.

Path Elements (QueryElement)
   A path element specifies a set of nodes and/or data attachments. A prefix can optionally
   be added to a path element. The prefix "(number)" limits the nodes returned for each query worker.
   So if you specify (10) on a cluster with 10 query workers the result should contain 100 elements
   or less. The prefix "(number-number)" skips the first N values and returns the second N values.
   The prefix "~" allows children nodes to continue to be processed when zero nodes are returned
   at the current level.

   ::

       /(10)+               Return up to 10 results from each node.
       /(5-10)+             Skip the first 5 results and return up to 10 results from each node.

Reserved Path Elements (QEReserved):
   There are three reserved path elements: "\*" matches all the nodes at the current level.
   "+" matches and collects all the nodes at the current level. ".." moves to the parent level
   of the tree. "+.." moves to the parent level of the tree and collects the parent level
   of the tree.

   ::

       /*                   Match against all the nodes at the current level.
       /+                   Match and collect all the nodes at the current level.
       /..                  Travel to the parent level.

Non-reserved Path Elements (QENode):
   The remaining (non-reserved) path elements are specified by an optional regular
   expression prefix, followed by a list of components, followed by zero or more
   property lists or fields. The regular expression prefix "|" indicates that the
   list of components should be interpreted as regular expressions. The regular expression
   prefix "!" returns the negation of the list of components interpreted as regular
   expressions.

   ::

       /foobar              Match against the node "foobar"
       /|.*foobar.*         Match against the regular expression ".*foobar.*".
       /!.*foobar.*         Match against the negation of the regular expression.
       /|+.*foobar.*        Match and collect nodes with the regular expression ".*foobar.*".

Path Components (QENComponent):
   The list of components are separated by either commas or semi-colons. Each component
   can have an optional prefix. These are three component prefixes: "+" collects the specified
   component. "++" performs prefix matching with the component and collects all
   the matches. "+{number} collects the specified component and inserts it into the column
   declared by the number.

   ::

       /a,b,c               Match against the nodes "a", "b", and "c"
       /+a,b,c              Match and collect the nodes "a", "b", and "c"
       /++a,b,c             Match and collect the nodes with prefix "a", "b", or "c"

Node Properties (QEPropertyList):
   A property is a data attachment that is intrinsic to all nodes. In other words
   it is a type of data attachment that all nodes contain. The property list is
   specified after the list of components using the notation ":prop1,prop2,etc".
   A property is collected by prefixing the property name with the "+" character.
   Five properties are recognized: "count" is an intrinsic counter associated with each
   node. "hits" is a synonym for "count". "nodes" is the number of children associated
   with the node. "mem" is a memory estimate for the node. "json" returns a json
   representation of the node. The properties can be filtered by one or more limits
   that are specified with "<", "=", or ">".

   ::

       /+:+hits             Collect all nodes and then collect the property "hits".
       /a:+hits>10          Match against node "a" and collect all hits greater than 10.

Data Attachments
----------------

   Data attachments are specified with one of two possible notations. The QEField notation
   begins with "$" and is written after the end of a component list. The QENDataAttachment notation
   begins with "+%" and is written in a component list instead of the name of a node. In
   general, the "$" notation is used to access values within a data attachment and the "+%"
   notation is used to create artificial nodes from within a data attachment. The "+%" notation
   accesses the data attachment associated with the nodes in the previous path element.

   Both notation can be passed parameters with using the "=" after the name of the data
   attachment. In the QEField notation the value that is returned may be filtered using the "<" or ">"
   notation after the name of the parameter. In the QENDataAttachment notation each of the
   different types of data attachments is responsible for handling the remainder of the
   string after the "=" character.

   ::

       httpd/ymd/+/urls/+$+xfer=sum$+xtime=sum

       Collect the values associated with the "xfer" and "xtime" data attachments.
       Each of the data attachments is passed the parameter "sum".

       httpd/ymd/+/urls/www.example.com/+%top_params:+hits

       Collect the top_params data attachment associated
       with the node "www.example.com" and collect the "hits"
       property on the created nodes.

Ops
###

The path expression provides a series of rows.  Operations (or ``ops``) are applied to those rows to provide a final result.  For example the most commonly used op -- ``gather`` -- merges rows together::

  gather=ks

Would treat column 0 as a unique key and the second column as a number to add together.  So::

  A 1
  A 3
  B 2
  C 5
  B 1

becomes::

 B 3
 A 4
 C 5

If ``gather`` is the most common ``sort`` is a close second::

  gather=ks;sort=1:n:d

The same example::

  A 1
  A 3
  B 2
  C 5
  B 1

now becomes::

 C 5
 A 4
 B 3

The ``n`` stands for numeric, and the ``d`` for descending.

Ops are separated by semicolons and if they take options it's with ``=``. TBD java doc link

Remote Ops
##########

The query system has a master process that performs a scatter gather out to a bunch of query workers.  Operations can be applied both at the master and worker.  Operations performed at the worker are usually called ``rops``, although the exact same set of operations is available.  A common pattern is provide a ``gather`` operation on both.  While the worker side ``gather`` does not result in a globally unique result, it reduces the number of rows that are sent to the master.


Query Performance
#################

Gather vs Merge
---------------

One common question people have is what is the difference between gather and merge.  ``Gather`` is a in-memory operation.
It stores all of the records sent to it in memory and then performs the required operations on the full result set
in-memory.  ``Merge`` is an operator that works by comparing adjacent records.  ``Gather`` can be used when the result
set is small enough to fit in the memory of a machine but ``merge`` in combination with ``sort`` should be used on
larger result sets.  ``sort`` will automatically fail over to disk if the number of rows is too large to handle
in memory.  This means you can sort a very large number of rows without breaking the process.

To better understand how adjacency works with merge look at this example::

  A 1
  A 2
  B 3
  C 4
  B 5

With the operation ``merge=ks`` it becomes::

  A 3
  B 3
  C 4
  B 5

Notice that B 3 and B 5 were not merged, because they were not adjacent.

The following two operation sets will produce the same result set but the gather will be all in memory and the merge will be more memory friendly::

  gather=ks;
  sort=0;merge=ks

Sorting before the merge operations means all keys with the same value will be adjacent.  This gives you exactly the same result as the gather operation but without crushing memory.


Tables and Rows
---------------

Operations can run either row by row, or require accumulating the entire results table.  ``sort`` for example must have the entire result to make any sense, while ``fill`` (which fills in empty columns with a default) can act row by row.  This distinction can be important for very large queries that do not fit in memory (queries are streaming).  Usually the distinction can be ignored unless performance tuning is required.

Limiting Query Results
----------------------

Hydra trees can be very large.  There are two mechanisms you can use in queries to limit the number of records that a query returns.

Op Limit
    When a limit operations is included in your query operation list it will make sure that the number of records
    sent to the next operation (or the end consumer if it is the last operation in a chain of operations) is less than or equal to the value
    specified in the limit operation.  For example 'limit=10' would return 10 or fewer results.  The key thing to
    understand about the limit operation is that it is applied after all of the results have been collected from the previous
    operation.  A query that returns 10 million records will find all 10 million records and then apply the limit to that list.
    This is very useful when you want to get the top 10 results based on hit count.  If you sort by hit count and then apply
    the limit operation you will get the top 10 results from the global result set.

Path Limit
    When the query path includes a limit, e.g. product-tracking/ymd/(10)*:+hits, the query nodes will stop looking for records
    after that limit has been reached.  The results returned in the case are partial results.  This is very different from the
    behavior described in the 'op limit' section. Each query worker will find the first 10 results
    (if there are at least 10 to be found) and then return immediately. Even if the results are sorted by hit count there
    is not guarantee that those 10 results will be the 10 highest (or lowest) results.  The path limit is applied at the
    query worker.  If there are 10 workers and a path limit of 10 than the master will get between 0 and 100 results.

