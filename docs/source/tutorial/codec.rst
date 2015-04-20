.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _codec:

#########################
Codec & HOCON Job Configs
#########################


What is Codec/HOCON?
====================

`Codec <https://github.com/addthis/codec>`_ is a java serialization library by AddThis that adds more helpful features
on top of `typesafe config (HOCON) <https://github.com/typesafehub/config>`_ and jackson (JSON). All hydra jobs configs
are parsed by Codec to turn them into Java objects for running in hydra.

We often refer to our job config language internally as "HOCON", but it is really a specific dialect of HOCON that Codec
knows how to parse. For example, things like _default, _primary, _inline, plugins, globals, and the like are all
Codec extensions to HOCON.

Codec is how we load all the things you can use in a job - Bundle Filters, Value Filters, Path elements, etc.
They are all defined in .conf files in the hydra source code.


JSON vs HOCON
=============

Since July 2014, and hydra version 4.2.14, hydra has had support for some form of HOCON. However, prior to this release
the only job format supported was the JSON format.

JSON format jobs are actually somewhat of a misnomer. Most JSON jobs that we wrote internally at AddThis were not
actually "JSON" in the strict sense. They were more of a javascript format, as Codec 2 supported non-quoted
identifiers and javascript-style comments, both illegal in JSON.

JSON is actually a subset of HOCON, which means that any JSON hydra job is actually also a HOCON job. HOCON also
supports the non-quoted identifiers and comments that our "JSON" job format supported. So if you were
to ask "How do I convert a JSON job to HOCON" the answer is actually "Your job is already HOCON"!

Some key differences:
---------------------
* Values in HOCON don't have to be quoted. For most production hydra jobs, our convention is to use quotes anyways.
* Single quoted strings are not supported in HOCON, while they were in the JSON format. In HOCON, a single quoted
  string will result in the string including the single quote characters.
* Comments in HOCON can be made using either "//" or "#". Our JSON format only allowed "//".
* The JSON format allowed multi-line comments using "/* comment */" syntax. HOCON does not have multi-line comments.
* There is only one additional reserved keyword in HOCON: "include". You must not use "include" anywhere in a job config
  except in quoted strings, unless using it for its file include function.
* You must escape strings as in java. In the JSON format, you could do something like "\\" or '"', which would now need
  to be escaped like this: "\\\\" or "\\""
* In HOCON, you can specify "op" or "type" differently. The extra braces can sometimes feel like too much, but you don't
  end up using this syntax for many bundle filters, as often you can just simplify further. Example::

   # JSON
   {op:"require", filter:{}}

   # HOCON
   {require {filter:{}}}

How to Simplify Jobs
====================

A JSON formatted job is also a HOCON job if there are no outer curly braces. That's nice for gradually changing a job
over from using old conventions to some of the newer features and plugins that our HOCON format makes available. But
just what are those features? How can you take advantage of them and make your job simpler and smaller?

"dot syntax"
------------

You can replace nested types with a dot syntax. Example::

   # JSON nested code
   source:{
      type:"mesh2",
      mesh: {
         files:["some_files"]
      }
   }

   # HOCON equivalent
   source.mesh2.mesh.files:["some_files"]

Also note that the "type" field is allowed to be used just as another sublevel without specifying the word "type".
This also works for the "op" field for bundle filters and value filters. Example::

   # JSON
   {op:"field", from:"FIELD", filter:{op:"require", value:["some_value"]}}

   # HOCON
   {field {from:"FIELD", filter.require.value:["some_value"]}}

If there were multiple fields listed at some level, you can either list them all out at that level or just make
multiple line of config. Two examples of valid ways to do this::

   source.mesh2.mesh: {
      files:["some_files]
      dateFormat:"YYMMdd"
   }

   source.mesh2.mesh.files:["some_files"]
   source.mesh2.mesh.dateFormat:"YYMMdd"

Autofields
----------

Any bundle filter that takes an Autofield as a parameter can do deep bundle references, within maps and arrays. Here's
how to do a deep bundle reference::

   # get value for key "uid" in MAP_FIELD, require that it be hexadecimal
   {from:"MAP_FIELD.uid", require.match:"[0-9a-f]+"}

   # set FIRST_ITEM to the first value in the ARRAY_FIELD array
   {from:"ARRAY_FIELD.0", to:"FIRST_ITEM"}

Const types
-----------

Instead of using a field as the "from" value for bundle filter field, you can use a constant. Example::

   {from.const:"some value", to:"FIELD"}

Array auto conversion
---------------------

If you have an array of a single item, you can leave out the square brackets. Jackson (the codec parsing backend) will
automatically turn a single item into a length one array. Example::

   # JSON
   {op:"require", value:["one"]}

   # HOCON
   {require.value:"one"}

Default bundle filter: "field"
------------------------------

The bundle filter plugin type has a _default field specified as bundle filter "field". This means that whenever you
use a bundle filter, if you're using "field", you don't have to specify the type of bundle filter. Example::

   # JSON
   {op:"field", from:"FIELD", filter:{op:"set", value:"foo"}}

   # HOCON, explicit type
   {field {from:"FIELD", filter.set.value:"foo"}}

   # HOCON, implicit default type
   {from:"FIELD, filter.set.value:"foo"}

Leaving out "filter" from bundle filter field's value filters
-------------------------------------------------------------

You can leave out the word "filter" from the "field" bundle filter when using it without a name. Everywhere else, you
still need to specify "filter". Example::

   # with filter
   {from:"FIELD", filter.set.value:"foo"}

   # without
   {from:"FIELD", set.value:"foo"}

   # elsewhere, such as a path element, filter required
   {const:"PATH", filter.require.value:"foo"}

_primary fields
---------------

Many types have a _primary field set in their config. Primary fields allow you to specify that single default field
without naming it.Some examples are bundle filter "field" (but only if you use it
in its default form where you don't use the name "field"), (bundle filter and value filter) chain, contains, split, index,
require, exclude. Here are some examples::

   # JSON
   {op:"field", from:"FIELD", filter:{set:"foo"}}
   {op:"require", value:["foo"]}
   {op:"split", split:","}

   # HOCON equivalents
   {from:"FIELD", set:"foo"}
   {require:"foo"}
   {split:","}

You can look up the primary for a type in the hydra user reference.
(`AddThis <http://app-docs.clearspring.local/docs/hydra-user-reference/latest/>`_)
(`OSS <http://oss-docs.clearspring.com/hydra/latest/user-reference/>`_)

Plugins: extra-filters
----------------------

There is now the ability to make "plugins" for each type of pluggable thing in hydra, which can be made up of calls
to other already defined types from the documentation. There are several pre-defined value filter and bundle filter
plugins available to use in
`extra-filters <https://github.com/addthis/hydra/blob/master/hydra-filters/src/main/resources/extra-filters-src.conf>`_

To use extra-filters, add ``include "extra-filters"`` to your job config.

Some selected useful plugin filters available in extra-filters:
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

safely
......

A replacement for bundle filter ``{chain {failReturn:true filter:{}}}``

It has a primary field of "filter". Use it like this when you always want some filters to pass::

   {safely:[
      # some filters here
   ]}

if
.....

A replacement for bundle filter ``condition``. The conditional filter can be specified inline.

Comparison between how ``condition`` was used and how you can use ``if``::

   # condition
   {condition {
      ifCondition:some.conditional.filter{},
      ifDo:some.trueOperation{},
      elseDo:some.falseOperation{}
   }

   # if
   {
      if:some.conditional.filter{},
      then:some.trueOperation{},
      else:some.falseOperation{}
   }

has
....

A more aptly named bundle filter ``field`` for use when you only want to know if a field is defined (non-null).
Primary field is "from". Example: `{has:"FIELD_NAME}`

try
....

Run another filter if a bundle filter fails.

invert
......

Return the opposite of a bundle filter, e.g. return true if a field is not set: ``{invert.has:"SOME_FIELD"}``

time-to-date, time-to-date-ymd
..............................

Assuming you have a field named "TIME" that is represented in unix time, these will populate "DATE" or "DATE_YMD"
in specific formats we use often at AddThis.

is-not-empty
............

Quick shortcut for ``{from:"FIELD_NAME", empty.not:true}``

Plugins: make your own
----------------------

It's simple to make your own additional plugins. They can be defined in several places: job configs, macros,
and any file on the classpath, which you would use the "include" keyword to import. When a plugin becomes general
purpose, it is often moved to extra-filters or to the hydra config files directly.

A good place to start learning how to make plugins is looking at the
`extra-filters <https://github.com/addthis/hydra/blob/master/hydra-filters/src/main/resources/extra-filters-src.conf>`_
in the hydra git repository. Here's the basic structure of how to define a plugin::

   # example bundle filter
   # ("bundle-filter" is the type of pluggable thing we are creating)
   plugins.bundle-filter {
      # plugin name
      field-to-field2 {
         _class: chain # name of base class. chain, and field are common use cases
         # any special properties like _primary
         # properties of the base class you want to override in the plugin
         # let's copy the value in FIELD to FIELD2,
         # but only if we find "foobar" as a substring in the field.
         filter: [
            {from:"FIELD", to:"FIELD2", require.contains:"foobar"}
         ]
      }
   }

Plugins can also be defined by referencing a new class directly, for example creating MyCoolCoolBundleFilter java class
and then setting it up in a conf file like extra-filters, or in the hydra conf files directly.

hocon "macros" which are just functions (global vs underscores)

Substitution: What can underscores do for you?
----------------------------------------------

In addition to the plugin system which allows you to create new plugins for pluggable types, HOCON job configs also
allow you to do `substitution <https://github.com/typesafehub/config#uses-of-substitutions>`_. There is one
important thing to know about substitution in hydra: you need to put things you plan to use for substitution
in either the global namespace (`global.thing-to-substitute`) or prefix it with an underscore (`_thing-to-substitute`).

The reason this matters is that normally in a hydra job, the parser tries to read everything into a java object. The
only way it knows that something is not supposed to be coerced into a java object is for it not to be available to
parse. The global namespace removes it from the parsing by placing it elsewhere in the tree. And any underscore-prefixed
property is ignored for java objects, as it is used for special properties like _class, _primary, etc.

With HOCON substitution, you basically get job config reuse without duplicating your code. Here is an example
of this in practice, lifted from a real hydra job::

   _uidcount {
       uid.count:{key:"UID", max:50000000}
       uidage.count:{key:"UID_GOOD", max:50000000}
       uidage48.count:{key:"UID_SUPER_GOOD", max:50000000}
   }

   output.tree.root: [
      {const:"ROOT"}
      {branch:[
         [
            {const:"mobile", filter.has:"MOBILE_FLAG", data:${_uidcount}}
         ]
         [
            {const:"desktop", filter.has:"DESKTOP_FLAG", data:${_uidcount}}
         ]
      ]}
   ]

Config merging
..............

When doing substitution, you can combine several sections of config into the same object. For example, if you wanted
to have two different things that both had the same base config, you could create the base part, and then merge that
into your two other things. Here's an example of that in practice::

   _basething: {
       uid.count:{key:"UID", max:50000000}
   }

   _thing1: ${_basething}
   _thing1: {
      agent.key-top:{key:"USER_AGENT", size:100}
   }

   _thing2: ${_basething}
   _thing2: {
      geo.key-top:{key:"COUNTRY", size:100}
   }

Codec Config Files
==================

Currently existing hydra codec config files and their contents:

* extra-filters: `extra-filters-src.conf <https://github.com/addthis/hydra/blob/762ae6d8d1c34875d692a21eec3aa4b3b1ac1c3e/hydra-filters/src/main/resources/extra-filters-src.conf>`_
* bundle filters, value filters: hydra-filters `reference.conf <https://github.com/addthis/hydra/blob/a52c35fefeb061e82efa62f3ba9b6de7b1f3e110/hydra-filters/src/main/resources/reference.conf>`_
* paths: `hydra-paths.conf <https://github.com/addthis/hydra/blob/44296bef84d9f9c5acaa975df0770e0defc0eaeb/hydra-task/src/main/resources/hydra-paths.conf>`_
* tree node data, data attachments: hydra-data `reference.conf <https://github.com/addthis/hydra/blob/e4c51add05251ef6e44256f1b0eed6d32846e765/hydra-data/src/main/resources/reference.conf>`_
* input sources, stream bundleizers: `hydra-sources.conf <https://github.com/addthis/hydra/blob/e0969fc851ec5ffe5206bc51cc7c6a3cd4324768/hydra-task/src/main/resources/hydra-sources.conf>`_
* executables, job alerts: hydra-main `reference.conf <https://github.com/addthis/hydra/blob/a5d38d2ef35f61743264816365cb333105651c5d/hydra-main/src/main/resources/reference.conf>`_
