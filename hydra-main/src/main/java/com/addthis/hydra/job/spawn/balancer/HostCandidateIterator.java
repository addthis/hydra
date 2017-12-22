package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.WithScore;

public class HostCandidateIterator {
    private static final Comparator<WithScore<JobTask>> taskComparator =
            Collections.reverseOrder(Comparator.comparingDouble(t -> t.score));
    // note: this complex comparator is necessary because maps need comparators that are consistend with equals
    private static final Comparator<HostAndScore> hostAndScoreComparator = (h1, h2) -> {
        int result = Double.compare(h1.score, h2.score);
        if (result == 0) {
            return h1.host.getHostUuid().compareTo(h2.host.getHostUuid());
        }
        return result;
    };

    // because we need to get tasks given a job key we need spawn...
    private final Spawn spawn;
    private final HostManager hostManager;
    private final SpawnBalancer balancer;
    private final TreeSet<HostAndScore> sortedHosts;
    private final int taskScoreIncrement;
    // this will be memoized by getNewReplicaHosts, -1 is the "not set" value
    private int minScore = -1;

    public HostCandidateIterator(
            Spawn spawn,
            HostManager hostManager,
            SpawnBalancer balancer,
            IJob job,
            Map<HostState, Double> scoreMap) {
        this.spawn = spawn;
        this.hostManager = hostManager;
        this.balancer = balancer;
        this.sortedHosts = new TreeSet<>(hostAndScoreComparator);
        this.taskScoreIncrement = balancer.getConfig().getSiblingWeight();

        if(job != null) {
            for (JobTask task : job.getCopyOfTasks()) {
                for (String replicaHostId : task.getAllTaskHosts()) {
                    HostState host = hostManager.getHostState(replicaHostId);
                    Double score = scoreMap.getOrDefault(host, 0d);
                    scoreMap.put(host, score + this.taskScoreIncrement);
                }
            }
        }
        // create the sortedHosts priority queue which will be used for picking candidate hosts
        for (Map.Entry<HostState, Double> entry : scoreMap.entrySet()) {
            this.sortedHosts.add(new HostAndScore(entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Return a host chosen in order of zone preference, then score
     */
    public List<String> getNewReplicaHosts(int replicas, JobTask task, @Nullable String excludeHostUuid, boolean isReplica) {
        Collection<HostLocation> locations = new HashSet<>();
        // get current host locations used by live/replicas
        for (String replicaHostId : task.getAllTaskHosts()) {
            if (replicaHostId != null && !replicaHostId.equals(excludeHostUuid)) {
                locations.add(hostManager.getHostState(replicaHostId).getHostLocation());
            }
        }

        List<String> chosenHostIds = new ArrayList<>(replicas);
        // create each replica
        for (int i = 0; i < replicas; i++) {
            int minScoreSoFar = Integer.MAX_VALUE;
            HostAndScore bestHost = null;
            Set<String> ads = new HashSet<>();
            Set<String> racks = new HashSet<>();
            Set<String> phys = new HashSet<>();
            for (HostAndScore hostAndScore : sortedHosts) {
                HostState host = hostAndScore.host;
                if (this.minScore == -1) {
                    HostLocation hostLocation = host.getHostLocation();
                    ads.add(hostLocation.getDataCenter());
                    racks.add(hostLocation.getRack());
                    phys.add(hostLocation.getPhysicalHost());
                }
                boolean excluded = host.getHostUuid().equals(excludeHostUuid);
                if (excluded || !host.canMirrorTasks() || (isReplica && !balancer.okToPutReplicaOnHost(host, task))) {
                    continue;
                }
                HostLocation hostLocation = host.getHostLocation();
                int score = 0;
                // HostLocation further away from task and replica locations gets lower score
                for (HostLocation location : locations) {
                    score += hostLocation.assignScoreByHostLocation(location);
                }
                if ((score == 0) || (score == this.minScore)) {
                    bestHost = hostAndScore;
                    break;
                }
                if (score < minScoreSoFar) {
                    // should cache this host as it's further away than the previous best host we've seen
                    bestHost = hostAndScore;
                    minScoreSoFar = score;
                }
            }
            this.cacheMinScore(ads, racks, phys);
            if (bestHost != null) {
                this.sortedHosts.remove(bestHost);
                this.sortedHosts.add(new HostAndScore(bestHost.host, bestHost.score + this.taskScoreIncrement));
                locations.add(bestHost.host.getHostLocation());
                chosenHostIds.add(bestHost.host.getHostUuid());
            }
        }
        return chosenHostIds;
    }

    // fixme: minAdScore should be a thing that comes from an enum that is calculated by spawn or hostmanager
    @SuppressWarnings("FloatingPointEquality") public @Nonnull Collection<JobTask> getTasksToMove(
            String fromHostUuid,
            String toHostUuid,
            int numTasks,
            int minAdScore,
            boolean obeyDontAutobalanceMe) {

        // loop through all tasks
        // cache the numScore lowest scores that can validly move
        // this is hard
        // we can store in a TaskAndScore type thing in a sorted list...
        // or... PriorityQueue with the same - it is free to look at the first/last element
        // if we find numScore that have minscore and can validly move, just return them immediately

        PriorityQueue<WithScore<JobTask>> sortedTasks = new PriorityQueue<>(numTasks, taskComparator);
        HostState fromHost = hostManager.getHostState(fromHostUuid);
        HostLocation fromHostLocation = fromHost.getHostLocation();
        HostLocation toHostLocation = hostManager.getHostState(toHostUuid).getHostLocation();
        for (JobKey jobKey : fromHost.allJobKeys()) {
            // todo: add all checks from findTasksToMove here
            JobTask task = spawn.getTask(jobKey);
            Job job = spawn.getJob(jobKey);

            // note some of these checks include:
            // - Only add non-null tasks that are either idle or queued
            // - Only add tasks that are supposed to live on the fromHost.
            if ((job != null) && (task != null) && SpawnBalancer.isInMovableState(task) &&
                (fromHostUuid.equals(task.getHostUUID()) || task.hasReplicaOnHost(fromHostUuid))) {
                if (obeyDontAutobalanceMe && job.getDontAutoBalanceMe()) {
                    // obeyDontAutobalanceMe is set to false when spawn is doing a filesystem-okay host failure.
                    // In this case, spawn needs to move the task even if the job owner specified no swapping,
                    // because the box is likely to be ailing/scheduled for decommission.
                    // All rebalancing actions use obeyDontAutobalanceMe=true and will conform to the job owner's
                    // wishes.
                    continue;
                }
                // todo:
                // valid means: has at least one copy in the minimum level of ad awareness
                // also passes all normal checks

                double taskScore = this.calculateTaskScore(fromHostLocation, toHostLocation, task);
                double highestScore = sortedTasks.peek().score;
                if (sortedTasks.isEmpty() || (highestScore > taskScore)) {
                    // add to queue if it's empty or this task has a lower score than the task with the largest score
                    sortedTasks.add(new WithScore<>(task, (double) taskScore));
                } else if ((sortedTasks.size() == numTasks) && (highestScore == (double) minAdScore)) {
                    // exit loop and return tasks if we have enough tasks and the largest score equals the min score
                    break;
                }
                if (sortedTasks.size() > numTasks) {
                    // remove largest score after pushing on a lower score
                    sortedTasks.poll();
                }
            }
        }
        // this could contain 0 - numTasks tasks.
        return sortedTasks.stream().map(WithScore::getElement).collect(Collectors.toList());
    }

    public List<String> getNewReplicaHosts(int replicas, JobTask task) {
        return this.getNewReplicaHosts(replicas, task, null, true);
    }

    /**
     * Get new replica host for a rebalancing task
     */
    public List<String> getNewReplicaHosts(JobTask task, @Nullable String excludeHost) {
        return this.getNewReplicaHosts(1, task, excludeHost, true);
    }

    /**
     * Calculate the minScore if it has not already been done.
     * This prevents iterating through every host every time we call getNewReplicaHosts.
     */
    private void cacheMinScore(Collection<String> ads, Collection<String> racks, Collection<String> phys) {
        if (this.minScore != -1) {
            return;
        }
        int numHosts = this.sortedHosts.size();
        int multiplier = 3;
        if (phys.size() > 1) {
            multiplier = 2;
        }
        if (racks.size() > 1) {
            multiplier = 1;
        }
        if (ads.size() > 1) {
            multiplier = 0;
        }
        this.minScore = multiplier * numHosts;
    }

    /**
     * Calculate the availability-domain-score of a task moving from one host to another.
     * A lower score is a better move to make, because it most distant.
     */
    private double calculateTaskScore(HostLocation fromHostLocation, HostLocation toHostLocation, JobTask task) {
        // exclude fromHostLocation
        // compare task locations with toHostLocation
        int score = 0;
        int numTasks = 0;
        for (String hostUuid : task.getAllTaskHosts()) {
            HostLocation taskHostLocation = hostManager.getHostState(hostUuid).getHostLocation();
            if (!taskHostLocation.equals(fromHostLocation)) {
                score += toHostLocation.assignScoreByHostLocation(taskHostLocation);
                numTasks++;
            }
        }
        // normalize score between jobs with different numbers of tasks
        // the -1 is for the excluded fromHostLocation
        return (double) score / (double) numTasks;
    }
}
