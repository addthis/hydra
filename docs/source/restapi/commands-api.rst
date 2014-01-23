.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. title:: Commands API

.. _commandsapi:

#############
Commands API
#############

``/command/list``
====================
Verb: GET

Returns the list of all the commands defined in the cluster in a json format.

``/command/get``
====================
Verb: GET

Returns a command in json format.

QueryParam:
 * id: The name or id of the command. Required.

``/command/save``
====================
Verb: POST

Updates or creates a new command with the specified names and jobs.

QueryParam:
 * name: The name or id of the command. Specified as a one string with no spaces.Required.
 * command: The actual command. Required.
 * owner: The owner of the command. Default: unknown.

``/command/delete``
====================
Verb: GET

Delete's the specified command.

QueryParam:
 * name: The name or id of the command. Specified as a one string with no spaces.Required.


.. _restapi:
