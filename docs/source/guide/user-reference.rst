.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _user-reference:

#######################
Writing User Reference
#######################

We use a number of custom Javadoc tags to control the presentation of the documentation output.  These are used to generate the :user-reference:`user reference <>` site. This page explains each of the tags and tell you how to use them. Note that all the standard Javadoc tags continue to work, and you are encouraged to use them wherever it is appropriate.

Class documentation
===================

The typical Javadoc documentation for a class will look like the example below. Use the standard `Javadoc tags <http://docs.oracle.com/javase/6/docs/technotes/tools/windows/javadoc.html#javadoctags>`_ wherever it is appropriate, such as the ``@link`` tag below. Javadoc does not automatically format paragraphs so remember to include ``<p>`` tags. All classes should have one or more examples that show they are used in a Hydra job. The examples are formatted using the ``<pre>`` tag.

.. code-block:: java

    /**
     * This {@link ValueFilter ValueFilter} <span class="hydra-summary">performs regular 
     * expression matching on the input string</span>.
     *
     * <p>The default behavior is to perform regular expression matching on the input string,
     * and return an array with all the substrings that match to the regular expression.
     * If the {@link #replace replace} field is used, then all substrings that match against
     * the pattern are replaced with the replacement string and the output is a string.
     *
     * <p>Example:</p>
     * <pre>
     *   {from:"SOURCE", to:"SOURCE", filter:{op:"regex", pattern:"Log_([0-9]+)\\."}},
     * </pre>
     * @user-reference
     * @hydra-name regex
     */

Field documentation
====================

You should document each fields that has the Java attribute ``@Set(codable = true)``. You must include the Javadoc documentation immediately above the Java attribute. Some grammatical suggestions:

* Use complete sentences. 
* Do not begin your sentence with an article ("a", "the", "some", etc.).
* If the Java attribute is ``@Set(codable = true, required = true)`` then include the sentence "This field is required."
* The default value should be explained with the last sentence 'Default is 1.', or 'Default is false.', or 'Default is either "dataSourceMeshy2.buffer" configuration value or 128.', etc.  

Here is an example of field documentation:

.. code-block:: java

        /** Regular expression to match against. This field is required. */
        @Set(codable = true, required = true)
        private String pattern;

        /** If non-null, then replace all matches with this string. Default is null. */
        @Set(codable = true)
        private String replace;

Common tags
=============

These tags should appear in nearly every Javadoc page.

@user-reference
----------------

Unlike a traditional Javadoc system, the hydra user reference is an opt-in system instead of an opt-out system. There are a large number of classes in the Hydra project and only a small percentage of those classes are exposed to the users. If you want a class to appear in the documentation then you must include this tag in the Javadoc. Note the convention of placing any Javadoc tags at the end of the documentation.

@hydra-name
------------

Our JSON parsing system uses a bidirectional map to associate names (Strings) with Java classes. Hydra users are accustomed to use these names when they are referring to the Java classes.  So you need to explicitly annotate the name associated with a classIf you do not provide a name then the Java class name will be listed instead.

<span class="hydra-summary">text</span>
-----------------------------------------------------

Any text that is placed inside this span block will appear in the summary table for this particular category. For the previous example the text appears in the ValueFilter :user-reference:`summary <com/addthis/data/filter/value/ValueFilter.html>` page.

Rare tags
==========

These tags are needed less frequently.

@hydra-category
----------------

This Javadoc tag is rarely used. It specifies the top-level categories for the Javadoc output. Currently the categories are "Hydra Jobs", "Input Sources", "Bundle Filters", "Value Filters", "Output Sinks", "Path Elements", and "Data Attachments". The subclasses of a class that has been tagged with ``@hydra-category`` will appear underneath the appropriate category. If you need to add a new category then you must add ``@hydra-category`` to the appropriate class, and you need to update the implementation of the IndexBuilder class in the documentation generator. The relative ordering of the categories and the mapping of category names to category classes is hardcoded and should be moved into a configuration file.

@exclude
---------

Any inner classes of a class that has been tagged with ``@user-reference`` are included in the documentation by default. You can use the @exclude tag to eliminate inner classes that the Hydra user will never use.

@exclude-fields field1,field2,field3
--------------------------------------

Sometimes we want to document the codable fields of a parent class that will be duplicated in the documentation of all its subclasses. But we don't want those fields to appear in the documentation of the parent class, so we use the ``@exclude-fields`` tag to prevent those fields from appearing only in the specific class that is tagged. An example of its use is in the ValueFilter class.
