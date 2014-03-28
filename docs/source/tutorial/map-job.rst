.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _map-job:

################
TreeBuilder Job
################

Motivation
===========

A frequent use case for Hydra is to gather aggregate data about a select few fields from a large data set, to answer questions like "how many events happened on a given day" or "what were the top browsers on a particular day." A map job reduces a complex input data set to a tree structure that can describe various statistics about the data. For example, if the tree has a branch for each date followed by a branch for each country, then a user can see how many events happened on a particular date and country, which countries had the most events for a particular day, how traffic for a particular country varies by day, and so on.

Map Job Configuration
-----------------------

Map jobs share the `source` and `map` sections with the :ref:`split job <split-job>` config, so see that page for information about how to fill out those sections. The special part of map jobs is the `output` section. Here is a sample map job `output`:

Example A:

.. code-block:: javascript

	output:{

		type:"tree",
		timeField:{
			field:"TIME",
			format:"native",
		},
		root:{path:"SAMPLE"},
		features:[
		],

		paths:{
			SAMPLE:[
				{type:"value", key:"DATE_YMD"},
				{type:"value", key:"COUNTRY"},
			],
		},
	},

`type` and `timeField` are mainly boilerplate, specifying a tree output and the field to use to determine node age.

`root` references one of the items in the `paths` section, which will be described in detail later. In particular, that the element called `SAMPLE` in `paths` should be the root node of the tree and evaluated first.

`features` is an optional way to add modular features to your tree that can easily be turned on and off.

`paths` is the where the actual structure of the tree is defined. First there is the `SAMPLE` label referenced in the `root` field. Next, the tree will create a branch of nodes for each value of `DATE_YMD`. Underneath each of these nodes, there will be a node for each value of `COUNTRY` that occurred for that particular value of `DATE_YMD`. Each node has an individual counter called `hits` that describe how many events reached the given node, possibly a number of children corresponding to the next level of the tree, and possibly also a number of data attachments (described later.) An example is a good illustration of how this process works:

Suppose the map specified above begins processing data. The first record that comes in looks like `{"DATE_YMD":130101,"COUNTRY":"SPAIN"}`. The tree will first create a node named `130101` corresponding to the `{type:"value",key:"DATE_YMD"},` line in the config. This node will have its `hits` incremented to 1, since it has seen one event so far. Then the tree proceeds to the next level, which is described by `{type:"value", key:"COUNTRY"}`. The tree will create a child node beneath the `130101` node, called `SPAIN`. This node will have its `hits` incremented to 1.

Suppose a second record came in that looked like `{"DATE_YMD":130101,"COUNTRY":"UK"}`. The tree will notice that a node called `130101` already exists, and increment its `hits` to 2. Then it will create a new child node underneath `130101` called `UK`, and set its `hits` to 1.

In this way, incoming events will increment the individual layers of the tree according to the fields set in this events. One important thing to note is that, if an event has no set value for a particular field, the tree will cease processing it at that point in the tree. Using the preceding example, if an event came in with just `{"COUNTRY":"ITALY"}`, then the tree will not change at all because this event does not have a value set for "DATE_YMD". If, alternatively, an event came in with just `{"DATE_YMD":130103}`, then the node for `130103` will be created/incremented, but none of the child nodes will be changed because this event does not have a `COUNTRY` specified.

In the query section you will learn how to extract this data from the tree using a query.

More Tree Structure
--------------------

In addition to the `{type:"value", key:"FIELD_NAME"}` lines seen above, there are additional options for modifying the structure of a tree:

`{type:"const", value:"customnode"}` will insert a static node called `customnode` at that level of the tree. This is often useful for labeling different parts of a tree.

The `list` branch type allows different branches of a tree to have different structure. Consider this example:

Example B:

.. code-block:: javascript

	{type:"branch", list:[
	[
		{type:"const", value:"ymd"},
		{type:"value", key:"DATE_YMD"},
	],
	[
		{type:"const", value:"country"},
		{type:"value", key:"COUNTRY"},
	],
	]},


The top level of this tree has two nodes: one called `ymd` and another called `country`. Under the `ymd` node is a list of `DATE_YMD` values seen and their counts, and under the `country` node is a list of `COUNTRY` values seen and their counts. These two branches are entirely independent.

Nodes can have a `filter` field that changes which events can travel past that point in the tree. For example, the following node of the tree will reject all events that do not have `COUNTRY` set to US:

.. code-block:: javascript

  {type:"const", value:"usonly", filter:{op:"field", from:"COUNTRY", filter:{op:"require", value:["US"]}}},

Any additional branches after this node will be updated only for events that pass the `COUNTRY=US` filter. For more information about filters, see [the filter section](/dev/hydra_training_bundle_filters).

Additional Considerations
----------------------------

It's easy to add any number of levels that look like `{type:"value", key:"FIELD_NAME"}` to continue to break down the data. However, each additional level in the tree will add some cost in terms of storage and query performance. Certain fields that take on many different values, such as **UID**, **PAGE_URL**, and **USER_AGENT**, are extremely expensive to insert into the tree directly. There are various alternatives that are preferable for analyzing these kinds of fields:

- Only interested in the **top values of the chosen field**? Use a **keytop**.

Modifying the above example to use a keytop would look like:

.. code-block:: javascript

  {type:"value",key:"DATE_YMD", data:{topcountry:{type:"key.top",key:"COUNTRY",size:20}}},

Now, instead of storing all countries seen under a given date, the job stores only the top 20 occurring values by date and the counts of each. This is much less expensive than storing every single value, particularly with fields like **PAGE_URL**. `data` is the section of the node that specifies data attachments; `topcountry` is an arbitrary name for the country keytop, `key` describe which field is used to get the top values, and `size` describes how many total values to store. Given that keytops are somewhat approximate, it is a good idea to make `size` somewhat larger than the number of values you're interested in. For example, if you want the top 10, use a `size` of 25 to 50 to ensure the top 10 will be correct. The query section will describe how to query the data stored in a keytop.

- Only interested in the **number of unique values of the chosen field**? Use a **counter**.

Modifying the above example to use a counter would look like:

.. code-block:: javascript

  {type:"value",key:"DATE_YMD", data:{numcountries:{type:"count",ver:"hll", rsd=0.05,key:"COUNTRY"}}},

This node will store a count of unique country values seen by this node, rather than storing the full list. It uses an approximation algorithm similar to a bloom filter, so it uses much less space than storing the actual list of nodes. `numcountries` is the arbitrary name given to this data attachment, `rsd` is the approximate error of the counter, and `key` is the field that will be counted. Counters are particularly useful when finding **UID counts**, which are much too numerous to insert into the tree directly.

- Only interested in **testing whether a value has been seen**? Use a **bloom filter**.

Modifying the above example to use a bloom filter would look like:

.. code-block:: javascript

  {type:"value",key:"DATE_YMD", data:{countrybloom:{type:"bloom", key:"COUNTRY", max:250}}},

This node will update a bloom filter based on the values of `COUNTRY` that pass by the node. The user can use query operations to test whether a particular value was seen by the bloom. The `key` specified the field to enter into the bloom, and the `max` should be a reasonable upper bound on the number of different values that the field can take.

- Interested in all values, but **don't care about values below a certain activity threshold**? Use a **band-pass** or **sieve**.

There are certain fields that are useful to insert into the tree, but that have a lot of malformed/noise values that are only seen once. This is especially the case with **PAGE_DOMAIN** and **USER_AGENT**. For example, suppose in a sample of a million view events, there are events for 1,000 actual domains an average of 800 times each, and also 200,000 malformed garbage domains seen only one time each. If the user were to insert `PAGE_DOMAIN` into the tree directly, the branch would have 201,000 nodes, which is extremely inefficient for storage/performance. If the job instead threw away all domains that were seen only once, there would be only 1,000 nodes at that level of the tree, resulting in much better performance. Domains that are seen only once are often not very useful for analysis anyway, so throwing them away is usually a good idea.

There are two ways to perform the above operation using filters: the **band-pass** and the **sieve**. Each operation works by throwing away a certain number of events for each field value, then letting events through after at least a certain number have been seen. The code for each follows below:

.. code-block:: javascript

  {type:"const", value:"sieve", data:{sieve:{type:"key.sieve2", key:"PAGE_DOMAIN", saturation:30, tiers:[{bits:6000000,bitsper:4,hash:4},{bits:6000000,bitsper:4,hash:4},{bits:6000000,bitsper:4,hash:4}]}}},
  {type:"value", key:"PAGE_DOMAIN"},`

  {type:"value", key:"PAGE_DOMAIN", filter:{op:"field", from:"PAGE_DOMAIN", filter:{op:"band-pass", minHits:3}}},`

The difference between these options is that band-pass is exact at the cost of additional storage space, whereas sieve is approximate using bloom filters with more efficient storage. Use band-pass unless the number of values of the chosen field run in the hundreds of thousands, in which case sieve is a better choice. Each of the above solutions is configured to throw away the first three events for `PAGE_DOMAIN` that are seen, meaning that only values that are seen four or more times will make it through to the rest of the tree. To change the number of events to be thrown away, add or remove additional `{bits:6000000,bitsper:4,hash:4}` levels to the sieve, or change the `minHits` argument for the band-pass. 

..
   The [data attachment section](/dev/hydra_training_data_attachments) has more information about data attachments.

Some Final Notes
------------------

The ordering of a tree's branches matters for performance. Suppose we know a tree needs branches by `DATE_YMD`, `USER_AGENT`, and `COUNTRY`, and that the most frequent query will be about counts by country and day. Then the tree's branches should be ordered: `DATE_YMD`, `COUNTRY`, then `USER_AGENT`. If instead we were to do `DATE_YMD`, `USER_AGENT`, then `COUNTRY`, then any operations to fetch the number by `COUNTRY` for a particular day would have to travel down all of the `USER_AGENT` nodes, which are very numerous. In general, it is better to put fields that take on fewer different values at higher levels of the tree.

Changing a tree's structure in the job config will not move the data already processed to conform with the new structure. If a map job with output `{type:"value", key:"DATE_YMD"}` processes some data, and is later changed to have output `{type:"value", key:"COUNTRY"}`, then the tree will have a confusing combination of dates and countries at the root level of the tree. For this reason it is almost always necessary to clone a map job when making structural changes, rather than editing the existing job.

When a map job is run, it processes only data that is available at that point in time. To continue to stay up-to-date, it must be rerun regularly, generally using the job rekick parameter. After a job finishes processing once, it will keep track of the files it has already processed and start again from the same point the next time the job is ran. If a map job has run recently but seems to not have up-to-date data, look at the source job to see if it is errored, disabled, or hasn't run for a while. Both the map job and the feeding split job must be up-to-date for the tree to have the latest data.

When running a map job with more than one task, each individual task will process different shards of the data and will produce a different tree based on the data it saw. You will learn the query section how to aggregate the counts from the individual trees into a single count. To tell how the source job is sharded, look in the description of the split job specified in the `source`. The convention is to have a description that looks like `UID:100` if the data is sharded by `UID` and there are 100 shards total. A map job should not have more processing nodes than there are shards in the input data, because any additional tasks will not find any data to process.

There are some performance implications for how the source data is sharded. For example, suppose a map job needs data broken down by `PAGE_DOMAIN`. If the source data is sharded by `PAGE_DOMAIN`, then each individual `PAGE_DOMAIN` will be entirely contained by a single node. Thus, if there are 100 tasks in the job and 100,000 domains in the data set, then each individual task will have an average of 1,000 domains, the trees will be relatively small, and query performance will be relatively good. If, on the other hand, the data is sharded by `UID`, then data for each domain will be split among many tasks, each task will have a large number of domains in the tree, and query performance will be worse. Resharding the source data by `PAGE_DOMAIN` would be a big performance improvement in this case.
