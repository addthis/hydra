.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.



.. title:: Jobs API

.. _jobsapi:

#############
Jobs API
#############

``/job/list``
====================
Verb: GET

Returns the entire list of jobs in the form of a json array.


``/job/enable``
====================
Verb: GET

Enable or disable a list of jobs.

QueryParam:
 * jobs: The jobs that need to be enabled. Specified as a comma separated list of job IDs.
 * enable: Enable or disable? Specified as 1 for enable, and 0 for disable. Default: 1.

``/job/rebalance``
====================
Verb: GET

Rebalance a job.

QueryParam:
 * id: The jobs that need to be rebalanced. Specified as a comma separated list of job IDs.

``/job/expand``
====================
Verb: GET

Return the expanded configuration of a specified job (without macros or parameters declarations);

QueryParam:
 * id: The id of the job to expand.

``/job/validate``
====================
Verb: GET

Validates the configuration of a specified job;

QueryParam:
 * id: The id of the job to validate.

``/job/delete``
====================
Verb: GET

Delete a specific job.

QueryParam:
 * id: The job id of the job to be deleted.

Job Dependencies
================

Job dependencies are represented as a directed graph. An edge from job A to job B represents
a data dependency from job A to job B. It is easiest to understand the directed graph
as a representation of the flow of data across the jobs. Data flows along the direction
of the edge. To illustrate the four API endpoints let's use the following job dependency
graph as an example:

.. image:: /_static/dependencies/all.png
    :align: center
    :height: 200px

``/job/dependencies/sources``
=============================
Verb: GET

Returns a dependency graph of the transitive upstream jobs of the target.

QueryParam:
 * id: The job id of the target job.

.. figure:: /_static/dependencies/sources.png
    :height: 150px
    :align: center

    The sources of job E

``/job/dependencies/sinks``
===========================
Verb: GET

Returns a dependency graph of the transitive downstream jobs of the target.

QueryParam:
 * id: The job id of the target job.

.. figure:: /_static/dependencies/sinks.png
    :height: 150px
    :align: center

    The sinks of job E

``/job/dependencies/connected``
===============================
Verb: GET

Returns a dependency graph of the transitive closure of all connected jobs to the target.

QueryParam:
 * id: The job id of the target job.

.. figure:: /_static/dependencies/connected.png
    :height: 200px
    :align: center

    All jobs connected to job E

``/job/dependencies/all``
=========================
Verb: GET

Returns a graph of all job dependencies in the cluster.

.. figure:: /_static/dependencies/all.png
    :height: 200px
    :align: center

    All jobs in the cluster

``/job/alerts.toggle``
======================
Verb: GET

Disable or enable job alerts.

``/job/get``
====================
Verb: GET

Returns information about a specific job in JSON format.

QueryParam:
 * id: The job id of the job. Required.
 * field: The fields of the job? Default to all fields   

``/job/stop``
====================
Verb: GET

Sends a stop signal to a job.

QueryParam:
 * id: The job id of the job. Required.
 * task: The task number of the task to be stopped. Default to -1 which means all tasks.

``/job/start``
====================
Verb: GET

Sends a kick signal to a job.

QueryParam:
 * id: The job id of the job. Required.
 * task: The task number of the task to be kicked. Default to -1 which means all tasks.

``/job/save``
====================
Verb: POST

Updates an existing job with the field values that are specified. If the job ID field is not specified, then it will create a new job. 

QueryParam:
 * id: The job id of the job. Required.
 * nodes: The number nodes or tasks of the job. Specified as the number of nodes. Required.
 * command: The command name of the job. Each command has a unique name. Required.
 * priority: The importance of the job relative to other jobs in the cluster. Specified as an integer >= 0. The higher the number, the higher the priority. Default: 0.
 * description: The description of the job. Optional.
 * onComplete: The URL the job should curl (GET) upon completion. Optional.
 * onError: The URL the job should curl (GET) if it errors. Optional.
 * config: The configuration of the job. Optional.
 * maxrun: The maximum duration of the job Specified as the number of minutes. Optional. Default: runs until job finished processing data. 
 * rekick: The number of minutes that the job should wait in idle state before automatically rekicking itself. Default: never rekicks itself.
 * dailyBackups: The number of daily backups the job should keep. Specified as integer >= 0. Default: 1.
 * hourlyBackups: The number of hourly backups the job should keep. Specified as integer >= 0. Default: 1.
 * weeklyBackups: The number of weekly backups the job should keep. Specified as integer >= 0. Default: 1.
 * monthlyBackups: The number of monthly backups the job should keep. Specified as integer >= 0. Default: 1.
 * replicas: The number of replicas the job should have. Specified as integer >= 0. Default: 1.
 * readOnlyReplicas: The number of read only replicas the job should have. Specified as integer>=0. Default:0.
 * qc_canQuery: Is the job queryable? Specified as true or false. Default: true.
 * qc_queryTraceLevel: Should the query have a log trace enabled? Specified as 1 for enabled, and 0 for disabled. Default:1.
