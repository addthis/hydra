.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. title:: Macros API

.. _macrosapi:


#############
Macro API
#############

``/macro/list``
====================
Verb: GET

Returns the entire list of macros in the form of a json array.

``/macro/get``
====================
Verb: GET

Returns a specified macro in json format.

QueryParam:
 * label: The unique name of the macro.

``/macro/save``
====================
Verb: POST

Returns a specified macro in json format. This endpoint requires a username in the header
of the POST request. It can be specified in curl with ``-H 'Username: hydra'``.

QueryParam:
 * label: The unique name of the macro. Specified as a short string of characters without spaces. Required.
 * description: A human readable description of the macro. A short description of macro with spaces allowed. Required.
 * macro: The content of the macro. Required.


``/macro/delete``
====================
Verb: GET

Deletes the specified macro.

QueryParam:
 * name: The unique name of the macro.
