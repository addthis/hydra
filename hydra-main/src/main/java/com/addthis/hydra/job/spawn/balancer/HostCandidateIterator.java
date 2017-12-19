package com.addthis.hydra.job.spawn.balancer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {
    private static HostManager hostManager;
    private static SpawnBalancer balancer;
    private PriorityQueue<HostAndScore> sortedHosts;
    private Set<HostLocation> taskAndReplicaLocations;

    HostCandidateIterator(HostManager hostManager, SpawnBalancer spawnBalancer, Comparator<HostAndScore> comparator) {
        this.hostManager = hostManager;
        this.balancer = spawnBalancer;
        this.sortedHosts = new PriorityQueue<>(1, comparator);
        this.taskAndReplicaLocations = new HashSet<>();
    }

    /**
     * Return a host chosen in order of zone preference, then score
     * @return
     */
    public HostState getNextHost() {
        HostAndScore bestHost = null;
        HostAndScore polledHost;
        int minScoreSoFar = Integer.MAX_VALUE;
        PriorityQueue<HostAndScore> copyQueue = new PriorityQueue<>(this.sortedHosts);

        while (!copyQueue.isEmpty()) {
            polledHost = copyQueue.poll();
            HostLocation hostLocation = polledHost.host.getHostLocation();
            int score = 0;
            // HostLocation further away from task and replica locations gets lower score
            for(HostLocation location : this.taskAndReplicaLocations) {
                score += hostLocation.assignScoreByHostLocation(location);
            }
            if(score < minScoreSoFar) {
                // should cache this host as it's further away than the previous best host we've seen
                bestHost = polledHost;
                minScoreSoFar = score;
            }
        }
        this.sortedHosts.remove(bestHost);
        this.sortedHosts.add(new HostAndScore(bestHost.host, bestHost.score + 25));
        this.taskAndReplicaLocations.add(bestHost.host.getHostLocation());
        return bestHost.host;
    }

    public boolean hasNextHost() {
        return !this.sortedHosts.isEmpty();
    }

    public void storeHostsByScore(Map<String, Double> scoreMap, JobTask task) {
        Set<String> replicaHostIds = task.getAllTaskHosts();
        for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
            HostState hostState = hostManager.getHostState(entry.getKey());
            if(replicaHostIds.contains(hostState.getHostUuid())) {
                this.taskAndReplicaLocations.add(hostState.getHostLocation());
                // Add 25 to scores of hosts with the task and replicas
                // to make it less likely to choose these hosts
                Double score = entry.getValue();
                entry.setValue(score + 25);
            }

            if (hostState == null || !(balancer.okToPutReplicaOnHost(hostState, task) && hostState.canMirrorTasks())) {
                continue;
            }
            this.sortedHosts.add(new HostAndScore(hostState, entry.getValue()));
        }
    }

}
