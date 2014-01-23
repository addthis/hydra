.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


Welcome to Hydra's documentation!
=================================

`Hydra <http://github.com/addthis/hydra>`_ is a distributed data processing and storage system originally developed at `AddThis <http://www.addthis.com/>`_.  It ingests streams of data (think log files) and builds trees that are aggregates, summaries, or transformations of the data. These trees can be used by humans to explore (tiny queries), as part of a machine learning pipeline (big queries), or to support live consoles on websites (lots of queries).

You can run hydra from the command line to slice and dice that Apache access log you have sitting around (or that mysterious gargantuan spreadsheet).  Or if terabytes per day is your cup of tea run a Hydra Cluster that supports your job with resource sharing, job management, distributed backups, data partitioning, and efficient bulk file transfer.

Other Documentation
===================

 * `source code <http://github.com/addthis/hydra>`_
 * `user reference <http://oss-docs.clearspring.com/hydra/latest/user-reference/>`_
 * `javadoc <http://oss-docs.clearspring.com/hydra/latest/javadoc/>`_
 * `maven-site <http://oss-docs.clearspring.com/hydra/latest/maven-site/>`_



Contents
==========

.. toctree::
   :maxdepth: 2

   concepts/index
   tutorial/index
   guide/index
   restapi/index
   about/index

..
   todo: 10 minutes to hydra as new quickstart

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

