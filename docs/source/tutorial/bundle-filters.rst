.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _bundle-filters:

###############
Bundle Filters
###############

As a Hydra Job developer Bundle Filters (and Value Filters) give you a powerful set of tools for manipulating records (bundles).  In Hydra the term 'bundle' refers to a single record of data.  Bundles are the inputs to Split and Map jobs and the return values from Hydra Queries.  Bundle Filters are tools Hydra provides to manipulate those records in some desirable way.  Bundle Filters have excess to all of the fields in a bundle.  They can create, edit, and delete fields in the bundle.  

A full list of the available bundle filters and documentation to explain their use can be found :user-reference:`here <>`.

..
   The `Hydra Tutor <http://hydratutor:8100/>`_ is a great resource for    exploring bundle filters.

Common Use Cases
================

Turning a date in string format into Unix Epoch time
-----------------------------------------------------

A very common need is to convert a date from `Unix time <http://en.wikipedia.org/wiki/Unix_time>`_ into a String based date.  Unix native time format is the preferred storage format for log files because parsing it is efficient for a computer and the time interpretation is unambiguous because unix time is always in GMT. While processing a record it is often useful to convert the unix time into a String based date format.  For example if we'd like to store the output by year/month/day we need to retrieve the year, month, and day from the unix time of the record. 

**INPUT RECORD**

.. code-block:: javascript

  {"time":"1367584938142", "k1":"v1", "k2":"v2"}

In the example input record is in unix time and we'd like to convert it to the format *yyMMdd-HHmmss* .  We will be using the `BundleFilterTime <com/addthis/data/filter/bundle/BundleFilterTime.html>`_ filter to accomplish this.

**BundleFilterTime**

.. code-block:: javascript

    {"op":"time", "src":{"field":"time", "format":"native"}, "dst":{"field":"DATE", "format":"yyMMdd-HHmmss", "timeZone":"EST"}}


Let's take a closer look at the filter.  The JSON configuration of the filter is defining an object that Hydra will decode into the correct BundleFilter instance type.  The first parameter in the configuration, *"op":"time"* tells Hydra to construct the BundleFilterTime object.  Looking at the JavaDoc for this object we see that it takes two parameters, *src* and *dst*.  Both the src and the dst are *TimeField* objects which are also defined as a JSON objects.  In our example the src is the src record read from the bundle and the name of that field is called *time* in our example.  The *TimeField* object requires that the *format* parameter is defined.  For unix time in milliseconds the *native* format is used.  The *dst* defines a new field, *DATE*, that will be added to our bundle and the requested format is *yyMMdd-HHmmss*.  Hydra's *TimeField* object uses JodaTime's `DateTimeFormat <http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html>`_ so it supports any format that JodaTime supports.

When the filter is applied the output is:

.. code-block:: javascript

  {"time":"1367584938142", "k1":"v1", "k2":"v2", "DATE":"130503-085533"}


Notice that the new field *DATE* has been added to the bundle.

User Agent Filters
-------------------

One of the primary use cases for Hydra is processing web log data.  Web logs typically include a user agent string that looks something like:

.. code-block:: text

    Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36


It is often useful to break down this string into its component parts so questions like:  

- "how many events from Chrome version 27 did we see?"
- "how many events from Macintosh computers did we see?"
- "what is the percentage of Intel events vs AMD?"

The recommended filter for parsing user agent strings is :user-reference:`user-agent3 <com/addthis/hydra/data/filter/bundle/BundleFilterUserAgentUADetector.html>`.  The JavaDoc for this filter is very good so please refer to that documentation for details on how it works.  Here we will just look at an example:

**INPUT RECORD**

.. code-block:: javascript

  {"first":"matt", "last":"abrams", "uastring":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36"}


**FILTER**

.. code-block:: javascript

  {"op":"user-agent3", "from":"uastring", "to":"ua"}


**OUTPUT**

.. code-block:: javascript

  {
    "first": "matt",
    "last": "abrams",
    "ua": "Chrome:27:OS X 10.8 Mountain Lion",
    "uastring": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36"
  }


As you can see the filter converted the string into its key component parts *Chrome:27:OS X 10.8 Mountain Lion*.  But we've just scrached the surface of what this filter can do.  Suppose in addition to the simplified UA string we'd also like to break out the relevant details of the UA into seperate fields.  

**FILTER**

.. code-block:: javascript

  {"op" : "user-agent3", "from" : "uastring",
      "uaName" : "UA_NAME", "osName" : "OS_NAME",
      "uaFamily" : "UA_FAMILY", "osFamily" : "OS_FAMILY",
      "uaMajorVersion" : "UA_MAJOR_VERSION", "osMajorVersion" : "OS_MAJOR_VERSION",
      "type" : "UA_TYPE", "deviceBrowserscope" : "UA_DEVICE", "to":"ua" },


**OUTPUT**

.. code-block:: javascript

  {
    "OS_FAMILY": "OS X",
    "OS_MAJOR_VERSION": "10",
    "OS_NAME": "OS X 10.8 Mountain Lion",
    "UA_DEVICE": "Other",
    "UA_FAMILY": "Chrome",
    "UA_MAJOR_VERSION": "27",
    "UA_NAME": "Chrome",
    "UA_TYPE": "Browser",
    "first": "matt",
    "last": "abrams",
    "ua": "Chrome:27:OS X 10.8 Mountain Lion",
    "uastring": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36"
  }


Bundle Filter Chains
=====================

Bundle filters are great and when something is great one is never enough.  :user-reference:`BundleFilterChain <com/addthis/hydra/data/filter/bundle/BundleFilterChain.html>` filters are filters that can chain a series of filters together.  

The key point to understanding bundle filter chains is that each BundleFilter returns a boolean value.  If the return value is true the next filter in the chain will be run and if it is false the chain will be halted.

One of the most basic (and common) bundle filters is called the :user-reference:`Field <com/addthis/data/filter/bundle/BundleFilterField.html>` filter.  There are a few parameters that can alter its default behavior but at a high level the filter returns true if the provided field exists in the record and false if it does not.  Lets suppose we have the following input records:

**INPUT RECORDS**

.. code-block:: javascript

  {"first":"matt", "last":"abrams", "uastring":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36"}
  {"first":"john","last":"doe"}


Notice the first record has a field for uastring but the second record does not.  We can use a chain to test to see if the uastring is present before calling the user agent filter.

**FILTER**

.. code-block:: javascript

  {"op":"chain", "filter":[
    {"op" : "field", "from": "uastring"},
    {"op" : "user-agent3", "from" : "uastring", to:"ua" }
  ]}


**OUTPUT**


.. code-block:: javascript

  {
    "first": "matt",
    "last": "abrams",
    "ua": "Chrome:27:OS X 10.8 Mountain Lion",
    "uastring": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.65 Safari/537.36"
  }
  {
    "first": "john",
    "last": "doe"
  }


This is a contrived example because the useragent filter would properly handle the missing field but the use case of needing to perform a test before executing a another filter is very common.

In some cases we want the chain to execute every filter regardless of the return values from the individual filters contained in the chain.  If this is the desired behavior we can set the *failStop* parameter to true.

**FILTER**


.. code-block:: javascript

  {"op":"chain", "failStop":"true", "filter":[
    {"op" : "field", "from": "uastring"},
    {"op" : "user-agent3", "from" : "uastring", to:"ua" }
  ]}


Exercises
==========

..
   *NOTE* you can test your bundle filters using the Hydra Tutor!

Exercise 1
------------

Create the JSON for a bundle filter to concatenate the first and last name into a new field called *firstlast* using a space to seperate the first and last names:

.. code-block:: javascript

  {"first":"matt", "last":"abrams"}


The output should look like:


.. code-block:: javascript

  {
    "first": "matt",
    "fullname": "matt abrams",
    "last": "abrams"
   }


Exercise 2
-----------

Given the following input records add a field called *bar* with value *foo* if field "first" is present in the bundle and if the field "first" is not present then add the field "foo" with value "bar".  Hint, you can use the "condition" bundle filter.

**INPUT**


.. code-block:: javascript

  {"first":"matt"},
  {"last":"abrams"}


**EXPECTED OUTPUT**

.. code-block:: javascript

  {"first":"matt", "bar":"foo"},
  {"last":"abrams", "foo":"bar"}


