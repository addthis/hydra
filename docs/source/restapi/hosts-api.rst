.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. title:: Hosts API

.. _hostsapi:

#############
Hosts API
#############

``/host/list``
====================
Verb: GET

Returns the entire list of hosts in the form of a json array.

``/host/rebalance``
====================
Verb: GET

Sends a rebalance signal to the specified host. 

QueryParam:
 * id: The UUID of the host that needs to be rebalanced.


``/host/fail``
====================
Verb: GET

Fails a host from the cluster.

QueryParam:
 * id: The UUID of the host that needs to be failed.

``/host/drop``
====================
Verb: GET

Drops a host from the cluster.

QueryParam:
 * id: The UUID of the host that needs to be dropped.

``/host/disable``
====================
Verb: GET

Disables a host from the cluster.

QueryParam:
 * id: The UUID of the host that needs to be disabled.

.. _restapi:
