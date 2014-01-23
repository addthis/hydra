.. Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied.  See the License for the specific language governing
   permissions and limitations under the License.


.. _spawn-balancing:

###############
Spawn Balancing
###############

Motivation
##########

One of the central purposes of the Hydra system is to split processing work and data storage between many minion machines. We get the most stability and throughput out of a cluster if each minion has a similar disk load and a similar portion of actively-running tasks.

However, due to the removal of failed hosts, the addition of new hosts, and other factors, we often end up with some very lightly-loaded minions and some heavily-loaded minions. The purpose of Spawn Balancing is to bring the hosts at these extremes closer to the mean and even out the work done by each minion. SpawnBalancer is the class that monitors host load and advises Spawn how to better balance tasks among machines.

Note: in the following description, the term "tasks" refers to both the live and replica versions of a task. The new Spawn Queue system frequently swaps tasks between live and replica status, so for balancing purposes they are considered equivalent.


Primary Functions
#################

SpawnBalancer provides the following services, which are described in more detail in the following sections:

 * Host Rebalancing - Given a host, decide if that host is heavily/lightly loaded relative to the other machines in the cluster. Then prescribe a list of moves to either push tasks onto that host or pull tasks off, as appropriate.

 * Job Rebalancing - Given a job, first check whether the job has the full count of replicas that the user asked for, and that each replica machine has a complete copy of the data. If these checks pass, then check the distribution of that job's tasks among the cluster's hosts, and prescribe a list of moves to push tasks around to ensure the spread is even.

In each case, the general model is that Spawn passes a snapshot of the current state to SpawnBalancer, which then advises Spawn on some tasks to move on the basis of this snapshot. Finally, Spawn checks whether each move is logical given the updated state, and executes the moves that are still valid. This is done so that SpawnBalancer can proceed without constantly checking the state of jobs, hosts, and tasks. In the event that a task moves, starts, or is deleted after the snapshot is sent to SpawnBalancer, Spawn will simply reject any move assignment involving that task.


Host Rebalancing
################

Host Rebalancing is a two phase process that first decides how the given host's load compares to the rest of the cluster, then moves tasks to even that host's load if appropriate.

When a request to rebalance a host comes in, SpawnBalancer first checks whether that host has significantly higher or lower disk usage than the majority of the other cluster machines. If so, then we perform Disk Space Balancing. Otherwise, we perform Active Tasks Balancing.

Disk Space Balancing
--------------------
SpawnBalancer decided that the given host did not have a fair amount of disk space used compared to the other hosts. To fix this situation, it first finds some hosts at the other extreme (for example, if the given host is very light, then it finds a few of the heaviest hosts on the cluster.) Next, it looks for eligible tasks to move from our heavy host(s) to our light host(s). Idle tasks are eligible only if:

 * they correspond to an actual existing Spawn job
 * the target host does not already have the maximum allowable number of tasks for that job
 * they are above a minimum size in bytes (to avoid moving lots of small tasks that aren't contributing much to disk usage)
 * they do not already exist on the target host
 * the author of the job did not specify that it should not be autobalanced
 
SpawnBalancer also restricts the overall number of tasks and bytes to be moved, to keep from overwhelming the hosts with a huge data copy.

Active Task Balancing
---------------------
Assuming the given host's disk space is not too out of line, the SpawnBalancer examines that host's portion of active jobs. For each job that has run within the specified time cutoff, compare the number of tasks on the given host to the overall number of tasks. SpawnBalancer determines a fair portion of tasks from this job using the overall number of tasks and live hosts with some wiggle room. For example, if there are 50 total tasks to be split among 9 hosts, having 5 or 6 tasks on a single host is acceptable; having 0 or 12 is not.

For each active job, SpawnBalancer carries out the following process:

 * Decide whether the given host has too many or too few tasks from that job, and how many tasks would have to be moved to give it a fair portion.
 * If the given host has much too many tasks, find one of the hosts that has the fewest number of tasks, find a task on the given host to move to the other host, then update the counts and continue.
 * If the given host has much too few tasks, do the opposite.
 * Otherwise, if the given host is generally very lightly loaded, consider putting an additional task on the given host. Do the opposite if the given host is generally very heavily loaded.
 * SpawnBalancer continues the preceding steps until each active job has a fair number of tasks on the given host, or until it exhausts the overall number of tasks to move per rebalancing.

Job Rebalancing
###############

In the first phase of job rebalancing, Spawn makes sure every task from the chosen job has its full count of replicas and that all the directories corresponding to the live/replica tasks are correct. Assuming these checks pass, the next phase of job rebalancing is very similar in purpose and implementation to the ACTIVE TASKS branch of host rebalancing, except that we consider a single job rather than a single host.

SpawnBalancer first finds a count of the number of tasks from the given job on each host in the cluster, and compares this count to a fair portion of tasks for this job. Then it applies this logic to each host in turn:

 * If the count greatly exceeds a fair portion, the host pushes some tasks onto a host that has few tasks.
 * If the count is at the maximum fair portion and the host is heavily loaded, push a task onto a host that has few tasks.

It is not necessary to particular consider hosts with too few tasks from this job. These hosts will get tasks pushed onto them simply by fixing the hosts with too many tasks.

As always, these moves are subject to sanity constraints: we can't move a task onto a host that already has a copy of it, or add any tasks to a host that has generally high load.








