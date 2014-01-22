.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _bundles-values:

########################
Data: Bundles and Values
########################


Bundles
########


A Hydra job might ingest binary data from a `Kafka`_ broker and emit a comma delimited file, but all incoming data is converted to an internal `Bundle`_ object.

.. _Kafka: https://kafka.apache.org/

.. _Bundle: https://github.com/addthis/bundle

Bundles are a representation of a packet, group, or other chunk of data.  In the common case of processing log lines each log line would become one bundle.  Bundles map ``BundleFields`` to values.  A common preamble to a job might look like:

.. code-block:: javascript

        fields:[
            {from:"TIME"},
            {from:"METHOD"},
            {from:"AGENT"},
            {from:"IP"},
        ],

Which is just saying to extract these specific fields from the incoming packet of data to work with (with the same name they had previously).

Bundles differ from a ``java.util.Map`` in that they they have a pre-bound format corresponding to the type of data that is being processed.  That means that no hashcode needs to be generated to lookup a value (under the hood it's usually array indexing).


Value Objects
##############

The "values" reverenced by bundle fields are of a few simple types:
 * String
 * Int
 * Float
 * Bytes
 * Array
 * Map
 * Custom

In Java-speak it was convenient to have a more meaningful common ancestor than ``java.lang.Object`` (all value objects know how to convert to each other for example, not just ``toString``).  We found that it was a worthwhile to have strong support for manipulating these basic types in succinct DSLs at the expensive of not allowing arbitrary object graphs and collections as input (which would require a full programing language to deal with anyway).

Maps and Arrays are compound values that contain more value objects within and can be iterated.  Arrays allow lookup by index while Maps support string keys.

ValueCustom
-----------

When something more exotic than wrapper around a primitive or simple collection is needed arbitrary classes can implement `ValueCustom`.  For example cardinality estimators maintain a complex bit field representation during processing but can be serialized as bytes or provide a simple count at :ref:`query time <queries>`.



Bundles, Bundles, everywhere
############################

In the ancient past every analytics process that consumed packets of data had a unique implementation.  We ended up re-implementation line based filtering in splitters, the same string parsing code when building trees, and a set of row operations for query results.  Bundles unify the interface for splits, trees, and queries and allow use of the same :ref:`filters <filters>` for all three.
