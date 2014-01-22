.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _maven-modules:

###############
Maven Modules
###############

Hydra is structured as a `mult-module <http://maven.apache.org/guides/mini/guide-multiple-modules.html>`_ maven project.  This allows flexibility for downstream project dependency management.  They can pull on the parts of hydra they need without getting the kitchen-sink.  It also allows for new hydra uber-jars to be built besides the defaults ``hydra-uber``. 

Don't want ``hydra-avro`` to get a slimmer jar, or need to exclude it to avoid a conflict with another package that uses avro?  Not a problem

Modules:

``hydra-essentials``
  Core classes for hash functions and the ClassMap module system.

``hydra-data``
  Data types, source, sinks, attachments, paths, and query operations.

``hydra-filters``
  Bundle and Value filters.

``hydra-store``
  On disk data storage; pagedb.

``hydra-mq``
  Rabbit and ZooKeeper message queue abstractions.

``hydra-task``
  File splitting and tree building.  Hydra tasks can be run stand alone (from the command line) without being part of a cluster.

``hydra-main-api``
  Core interfaces for manipulating Hydra jobs in a distributed system.

``hydra-main``
  Hydra processes: Spawn, Minion, QueryMaster, QueryWorker.

``hydra-avro``
  Optional classes for reading and writing Avro files.

``hydra-uber``
  Everything bundled together as an uber-jar.
