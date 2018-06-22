package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.WithScore;

public class HostCandidateIterator {
    // note: this complex comparator is necessary because maps need comparators that are consistent with equals
    private static final Comparator<WithScore<HostState>> hostAndScoreComparator = (h1, h2) -> {
        int result = Double.compare(h1.score, h2.score);
        if (result == 0) {
            return h1.element.getHostUuid().compareTo(h2.element.getHostUuid());
        }
        return result;
    };

    private final HostManager hostManager;
    private final SpawnBalancer balancer;
    private final TreeSet<WithScore<HostState>> sortedHosts;
    private final int taskScoreIncrement;
    private final int minScore;

    public HostCandidateIterator(Spawn spawn, IJob job, Map<HostState, Double> scoreMap) {
        this.hostManager = spawn.hostManager;
        this.balancer = spawn.getSpawnBalancer();
        this.sortedHosts = new TreeSet<>(hostAndScoreComparator);
        this.taskScoreIncrement = balancer.getConfig().getSiblingWeight();

        Map<HostState, Double> copyOfScoreMap = new HashMap<>(scoreMap);
        if(job != null) {
            for (JobTask task : job.getCopyOfTasks()) {
                for (String replicaHostId : task.getAllTaskHosts()) {
                    HostState host = hostManager.getHostState(replicaHostId);
                    Double score = copyOfScoreMap.getOrDefault(host, 0d);
                    copyOfScoreMap.put(host, score + (double) this.taskScoreIncrement);
                }
            }
        }
        // create the sortedHosts priority queue which will be used for picking candidate hosts
        for (Map.Entry<HostState, Double> entry : copyOfScoreMap.entrySet()) {
            this.sortedHosts.add(new WithScore<>(entry.getKey(), entry.getValue()));
        }

        this.minScore = hostManager.getHostLocationSummary().getPriorityLevel().score;
    }

    /**
     * Return a host chosen in order of zone preference, then score
     * There are two <i>scores</i> at work here: the "Host score", which is a measure of load on the host
     * and the "Host Location score", which is a measure of distance/separation between HostLocation(s).
     * The hosts selected as replica targets attempt to minimize HostLocationScore (see {@link HostLocation#assignScoreByHostLocation(HostLocation)})
     * and HostScore (see {@link SpawnBalancer#generateHostStateScoreMap(Collection)})
     */
    public List<String> getNewReplicaHosts(int replicas, JobTask task,
                                           @Nullable String excludeHostUuid, boolean isReplica) {
        if(replicas <= 0) {
            return new ArrayList<>();
        }

        Collection<HostLocation> locations = new ArrayList<>();
        // assemble a list of host locations currently holding live/replicas
        for (String replicaHostId : task.getAllTaskHosts()) {
            if ((replicaHostId != null) && !replicaHostId.equals(excludeHostUuid)) {
                locations.add(hostManager.getHostState(replicaHostId).getHostLocation());
            }
        }

        List<String> chosenHostIds = new ArrayList<>(replicas);
        // create each replica
        for (int i = 0; i < replicas; i++) {
            double minScoreSoFar = Double.MAX_VALUE;
            WithScore<HostState> bestHost = null;

            // Hosts are already sorted by HostScore
            // Choose the host that has the best HostLocationScore
            for (WithScore<HostState> hostAndScore : sortedHosts) {
                HostState host = hostAndScore.element;
                boolean excluded = host.getHostUuid().equals(excludeHostUuid);
                if (excluded || !host.canMirrorTasks() || (isReplica && !balancer.okToPutReplicaOnHost(host, task))) {
                    continue;
                }
                HostLocation hostLocation = host.getHostLocation();
                int score = 0;
                // HostLocation further away from task and replica locations gets lower score
                // Calculate the total distance between this host and all current live/replicas
                for (HostLocation location : locations) {
                    score += hostLocation.assignScoreByHostLocation(location);
                }
                double avgScore = locations.isEmpty()? 0 : (double) score / (double) locations.size();

                // This is the furthest distance possible with the hosts currently registred with HostManager
                if (avgScore == (double) this.minScore) {
                    bestHost = hostAndScore;
                    break;
                }
                if (avgScore < minScoreSoFar) {
                    // should cache this host as it's further away than the previous best host we've seen
                    bestHost = hostAndScore;
                    minScoreSoFar = avgScore;
                }
            }
            if (bestHost != null) {
                this.sortedHosts.remove(bestHost);
                this.sortedHosts.add(new WithScore<>(bestHost.element, bestHost.score + (double) this.taskScoreIncrement));
                locations.add(bestHost.element.getHostLocation());
                chosenHostIds.add(bestHost.element.getHostUuid());
            }
        }
        return chosenHostIds;
    }

    /**
     * Get new replica host
     */
    public List<String> getNewReplicaHosts(int replicas, JobTask task) {
        return this.getNewReplicaHosts(replicas, task, null, true);
    }

    /**
     * Get new replica host for a rebalancing task
     * note: this is not used at the moment, but saved for a later date when we rewrite spawnbalancer
     */
    public List<String> getNewReplicaHosts(JobTask task, @Nullable String excludeHost) {
        return this.getNewReplicaHosts(1, task, excludeHost, true);
    }

}
