.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


##################################
How to use the native query client
##################################

Most of use routinely use either the spawn Data browser or curl / wget to perform queries against the hydra query system but this is not always the case.  The query system has a native non-http protocol that is, in many cases, less troublesome than tunneling imperfectly through http.

First, find a hydra uber jar and setup an alias
alias hydra='java -jar ~/home/hydra/hydra-uber/target/hydra-uber-* -exec.jar'

Let's say we have a job 7fefed5a-68df-4087-80a3-dfacba906d93 that has N different values at level 1 of the tree and we want to query their counts.

.. code-block:: javascript

	hydra qutil 
	host=qm.example.local
	port=2601
	job=7fefed5a-68df-4087-80a3-dfacba906d93
	path="VALUES/+:+count"  ops="gather=ks"
	csv quiet > results.csv

A list of all available options include:
 * host=[host]
 * port=[port]
 * job=[job]
 * path=[path]
 * ops=[ops]
 * rops=[rops]
 * lops=[lops] : to be used when query locally on a minion
 * data=[datadir] : local data directory on a minion
 * [iter]
 * [quiet] : do not emit logs
 * [sep=separator]
 * [out=file] : set a output file
 * [trace]
 * [param=val]


Another use case is to run these queries locally on the minions containing the data so that they don't have to streamed back through the Query Master. This way, instead of one single fat file one can distribute the results across multiple machines. The syntax is similar to above but we need to specify the data directory and the local ops.

.. code-block:: javascript

	hydra qutil 
	data=data/
	job=7fefed5a-68df-4087-80a3-dfacba906d93
	path="VALUES/+:+count"
	lops="gather=ks"
	csv quiet > results.csv

