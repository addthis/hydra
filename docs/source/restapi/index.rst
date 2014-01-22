.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. title:: Rest API

.. _restapi:

#############
Rest API
#############

The HTTP API endpoint names try to follow the following naming convention:
 * /{noun}/{verb}
 * /{noun1}/{noun2}.{verb}

Unless otherwise specified all endpoints return json and should be called in the form http://host:port/rest-of-path.

.. toctree::
    :maxdepth: 1

    jobs-api
    hosts-api
    tasks-api
    alias-api
    macros-api
    commands-api
