.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. title:: Alias API

.. _aliasapi:

#############
Alias API
#############

``/alias/list``
====================
Verb: GET

Returns the list of all the aliases defined in the cluster a json format.

``/alias/get``
====================
Verb: GET

Returns the name and list of jobs of an alias, specified by its name in json format.

QueryParam:
 * id: The name or id of the alias. Required.

``/alias/save``
====================
Verb: GET

Updates or creates a new alias with the specified names and jobs.

QueryParam:
 * name: The name or id of the alias. Specified as a one string with no spaces.Required.
 * jobs: The list of job IDs that will have this alias. Specified as a comma separated list of job IDs. Optional.

``/alias/delete``
====================
Verb: GET

Delete's the specified alias.

QueryParam:
 * name: The name or id of the alias. Specified as a one string with no spaces.Required.


.. _restapi:
