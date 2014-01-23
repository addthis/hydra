.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _components-threads:

#########
Threads
#########

Jobs
-----

Since Hydra tasks can be arbitrary programs there is no single architecture they have to follow.  However, most treebuilder & split jobs will behave summarily regardless of sources or sinks.

A single ``SourceReader`` thread is the keystone.  It polls a queue for new bundles and puts them on an outbound queue for processor threads.  It's possible that there may be several threads pre-fetching, decompressing, or do other work to make data available to the ``SourceReader``.

Processor threads do the actual "work" of running filters before passing the bundles to an outbound sink queue.  This queue will also likely have multiple threads draining from it (for example, to compress and write out bundles to files).

If profiling, the processor threads are usually a good place to start:
 * If they are blocked waiting for data, the source is not pulling new data in fast enough.
 * If they are blocked on the sink (or doing work for the sink; a "caller runs" policy is typically used), the sink is not draining fast enough.
 * If they are running all the time applying filters the job may benefit from using more threads.
