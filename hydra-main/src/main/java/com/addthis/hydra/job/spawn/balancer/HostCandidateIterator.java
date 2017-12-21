package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {
    private static final Comparator<HostAndScore> hostAndScoreComparator = (h1, h2) -> {
        int result = Double.compare(h1.score, h2.score);
        if(result == 0) {
            return h1.host.getHostUuid().compareTo(h2.host.getHostUuid());
        }
        return result;
    };

    private final HostManager hostManager;
    private final SpawnBalancer balancer;
    private final TreeSet<HostAndScore> sortedHosts;
    private final int taskScoreIncrement;
    // this will be memoized by getNewReplicaHosts, -1 is the "not set" value
    private int minScore = -1;

    public HostCandidateIterator(
            HostManager hostManager,
            SpawnBalancer balancer,
            IJob job,
            Map<HostState, Double> scoreMap) {
        this.hostManager = hostManager;
        this.balancer = balancer;
        this.sortedHosts = new TreeSet<>(hostAndScoreComparator);
        this.taskScoreIncrement = balancer.getConfig().getSiblingWeight();

        for (JobTask task : job.getCopyOfTasks()) {
            for (String replicaHostId : task.getAllTaskHosts()) {
                HostState host = hostManager.getHostState(replicaHostId);
                Double score = scoreMap.getOrDefault(host, 0d);
                scoreMap.put(host, score + this.taskScoreIncrement);
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
    public List<String> getNewReplicaHosts(int replicas, JobTask task, @Nullable String excludeHostUuid) {
        Collection<HostLocation> locations = new HashSet<>();
        // get current host locations used by live/replicas
        for (String replicaHostId : task.getAllTaskHosts()) {
            if (!replicaHostId.equals(excludeHostUuid)) {
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
                if (excluded || !(balancer.okToPutReplicaOnHost(host, task) && host.canMirrorTasks())) {
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

    public List<String> getNewReplicaHosts(int replicas, JobTask task) {
        return this.getNewReplicaHosts(replicas, task, null);
    }

    /**
     * Get new replica host for a rebalancing task
     */
    public List<String> getNewReplicaHosts(JobTask task, @Nullable String excludeHost) {
        return this.getNewReplicaHosts(1, task, excludeHost);
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
}
