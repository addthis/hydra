.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _standards:

##############################
Code Formatting & Development
##############################

Java Code Style
###############

Deviations from standard practice

 * Do not use tabs
 * 4 space indent
 * Always use braces
 * Opening braces on the same line
 * Whitespace around operators
 * Never use on-demand imports (i.e. com.acme.foo.*)
 * Group imports
 * No author name in source files [#]_.

See the `checkstyle file <https://github.com/addthis/addthis-oss-jar-pom/tree/master/checkstyle>`_ for details.

.. [#] For the same reasons outlined by the Apache Subversion `community`_ 

.. _community: http://subversion.apache.org/docs/community-guide/conventions.html#other-conventions


License Headers
#################

All files should be under the ALv2 and have the a standard header.

.. code-block:: java

  /*
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

Version Control
###############

Pull requests or patches should be free of extraneous "merge" commits.  That is, you should rebase your branch on top of ``master`` instead of merging master into your branch.


Versions
########

It's ``x.y.x`` where:
 * x: Something major and momentous
 * y: Normal release.  Should be either backwards compatible or a reasonable upgrade plan.
 * z: Patches, bug fixes or very important backwards compactible changes



