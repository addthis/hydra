.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _filters:

############################
Filtering Bundles and Values
############################

Filters (aka functions, things that do stuff, etc) come in two varieties: those that act on
the entire bundle and those that act on a single value. Bundle filters mutate the underlying bundle
and return a boolean indicated either success/failure or that some predicate was satisfied.
Value filters perform an operation on a single value and return a single value.

They can be used together such as when a bundle filter might apply a value filter to a
certain subset of fields.   Both also support chaining where a series of filters are applied,
optionally allowing a ``false`` response to terminate the chain.

Bundle Filters
##############

Bundle filters can operate on the bundle as a whole. They are usually used for a many:many mapping.
For example, concatenating two fields together and storing the results in a 3rd field (:user-reference:`concat <com/addthis/hydra/data/filter/bundle/BundleFilterConcat.html>`):

.. code-block:: javascript

    {op:"concat", in:["FIRST","LAST"], out:"FULL_NAME", join:" "},

The ``FULL_NAME`` field can then be used latter just as the original fields from the source.
Another common use cases is for pulling apart URLs (:user-reference:`url <com/addthis/hydra/data/filter/bundle/BundleFilterURL.html>`):

.. code-block:: javascript

  {op:"url", field:"URL", setHost:"DOM", setPath:"PATH", setParams:"PATH_PARAMS"}

This results in 3 new fields -- for the domain, path, and parameters -- to be used later.
For example, we might build a :ref:`tree <paths-trees>` that breaks URLs down by domain.

Value Filters
##############

Value Filters operate on a single implicit input field (:user-reference:`default <com/addthis/hydra/data/filter/value/ValueFilterDefault.html>`).

.. code-block:: javascript

  filter:{op:"default",value:"unknown"}

In this case if the input field is not set then a default value is provided. They are often
used in conjunction with other filters in chains.

.. code-block:: javascript

	{op:"map-extract", field:"QUERY_PARAMS", map:[
		{from:"SOME_INPUT_PARAM",to:"PAGE_DOMAIN",filter:{op:"chain",filter:[
			{op:"split",split:":"}, // host:port
			{op:"index",index:0}, // host
			{op:"case",lower:true}, // housekeeping
		]}},
	]},

``map-extract`` is a bundle filter that pulls apart the ``QUERY_PARAMS`` value map
into individual fields for later use. We clean up the input along the way
with a chain of value filters (:user-reference:`split <com/addthis/hydra/data/filter/value/ValueFilterSplit.html>`, :user-reference:`index <com/addthis/hydra/data/filter/value/ValueFilterIndex.html>`, :user-reference:`case <com/addthis/hydra/data/filter/value/ValueFilterCase.html>`).
