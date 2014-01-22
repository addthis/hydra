.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _shards-pipelines:

########
Shards
########


Hydra jobs typically use shards (sometimes called bucketing or mods) to determine which job :ref:`tasks <clusters-jobs>` should process what data.  Typically a job will have a single output key (such as ``URL``, ``IP``, or ``TIME``) that it hashes on to determine the output shard.   Shard 0 data will be written to one set file, while shard 7.  Subsequent split jobs may take data sharded on one key and output it sharded on a different key. 

Each task is given how many total tasks are in a job and it's ID, from that and the number of input shards it can determine which input files to consume.  Taking advantage of the presence of the shard id in the file name allows downstream jobs to only look at files that match it's task id.

Shard choices can have significant implications for processing time and query performance of jobs.  See :ref:`performance tuning <performance-tuning>` for details.


