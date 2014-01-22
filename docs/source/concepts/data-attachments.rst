.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _data-attachments:

################
Data Attachments
################

Tree nodes can have data attached to them.  Some of the most comonly used include:

 * Bloom Filter
 * Cardinality Estimator
 * Secondary Counts (think views and bytes for URLs)
 * Creation or touch time (for TTLs)
 * Top-k Estimator
 * Histograms

Data attachments can be purly informational (such as a ``sum``) or affect the rest of a tree.  A ``limit.top`` limits how many children a node can have.  Continuing our product code example.  For each version of a product we might want to keep track of how many domains that product is used for, and a small sample of the most popular domains, but not store every single domain.  Cardinality and top-k estimators allow you to do that:

.. code-block:: javascript

    {type:"value", key:"PRODUCT_VERSION", data:{
        cdom:{type:"count", key:"PAGE_DOMAIN", max:5000000},
        dom:{type:"key.top", key:"PAGE_DOMAIN", size:200},
        uid:{type:"count", key:"UID", max:50000000},
        bytes:{type:"sum", key:"XFER_BYTES"},
        }},

