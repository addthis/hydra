.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _gdrive:

######################
Export to Google Drive
######################

#. Create a new project in the Google API Console. The instructions are
   `here <https://developers.google.com/console/help/#creatingdeletingprojects>`_.
   For typical usage you are unlikely to require billing services.

#. Enable the "Drive API" and "Drive SDK" services.

#. Create a new client ID under "Credentials" / "OAuth". You will want to create a
   Client ID for web application. The Javascript origins can be empty. The
   Redirect URIs must include the following two URIs for each server that will
   run the query master proccess: ``http://[hostname]:2222/query/google/authorization`` and
   ``http://[hostname]:2222/query/google/submit``. We recommend adding localhost as a server for testing purposes.

#. Set the following system parameters for the query master Java process:
   ``qmaster.export.gdrive.clientId`` and ``qmaster.export.gdrive.clientSecret``.
