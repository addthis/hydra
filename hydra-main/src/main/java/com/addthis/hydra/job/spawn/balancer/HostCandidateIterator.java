package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {
    private final HostManager hostManager;
    private final SpawnBalancer balancer;
    private final TreeSet<HostAndScore> sortedHosts;
    // fixme: use spawnbalancerconfig and get value for this
    private static final double TASK_SCORE_INCREMENT = 25;

    // fixme: can we reduce # of parameters in constructor?
    HostCandidateIterator(
            HostManager hostManager,
            SpawnBalancer spawnBalancer,
            Comparator<HostAndScore> comparator,
            Job job,
            Map<String, Double> scoreMap) {
        this.hostManager = hostManager;
        this.balancer = spawnBalancer;
        this.sortedHosts = new TreeSet<>(comparator);
        Map<String, Double> scoreMapCopy = new HashMap<>(scoreMap);
        // copy the score map, add 25 to each score per task/replica on a host
        for (JobTask task : job.getCopyOfTasks()) {
            for (String replicaHostId : task.getAllTaskHosts()) {
                Double score = scoreMapCopy.get(replicaHostId);
                scoreMapCopy.put(replicaHostId, score + TASK_SCORE_INCREMENT);
            }
        }
        // create the sortedHosts priority queue which will be used for picking candidate hosts
        for (Map.Entry<String, Double> entry : scoreMapCopy.entrySet()) {
            HostState host = hostManager.getHostState(entry.getKey());
            if (host != null) {
                this.sortedHosts.add(new HostAndScore(host, entry.getValue()));
            }
        }
    }

    /**
     * Return a host chosen in order of zone preference, then score
     */
    public List<HostState> getNewReplicaHosts(int replicas, JobTask task, @Nullable HostState excludeHost) {
        Collection<HostLocation> locations = new HashSet<>();
        // get current host locations used by live/replicas
        for (String replicaHostId : task.getAllTaskHosts()) {
            locations.add(hostManager.getHostState(replicaHostId).getHostLocation());
        }

        List<HostState> chosenHosts = new ArrayList<>(replicas);
        // create each replica
        for (int i = 0; i < replicas; i++) {
            int minScoreSoFar = Integer.MAX_VALUE;
            HostAndScore bestHost = null;
            for (HostAndScore hostAndScore : sortedHosts) {
                HostState host = hostAndScore.host;
                boolean excluded = host.getHostUuid().equals(excludeHost.getHostUuid());
                if (excluded || !(balancer.okToPutReplicaOnHost(host, task) && host.canMirrorTasks())) {
                    continue;
                }
                HostLocation hostLocation = host.getHostLocation();
                int score = 0;
                // HostLocation further away from task and replica locations gets lower score
                for (HostLocation location : locations) {
                    score += hostLocation.assignScoreByHostLocation(location);
                }
                if (score == 0) {
                    bestHost = hostAndScore;
                    break;
                }
                if (score < minScoreSoFar) {
                    // should cache this host as it's further away than the previous best host we've seen
                    bestHost = hostAndScore;
                    minScoreSoFar = score;
                }
            }
            if (bestHost != null) {
                this.sortedHosts.remove(bestHost);
                this.sortedHosts.add(new HostAndScore(bestHost.host, bestHost.score + TASK_SCORE_INCREMENT));
                locations.add(bestHost.host.getHostLocation());
                chosenHosts.add(bestHost.host);
            }
        }
        return chosenHosts;
    }

    public List<HostState> getNewReplicaHosts(int replicas, JobTask task) {
        return getNewReplicaHosts(replicas, task, null);
    }

    public List<HostState> getNewReplicaHosts(JobTask task, @Nullable HostState excludeHost) {
        return getNewReplicaHosts(1, task, excludeHost);
    }

    public boolean hasNextHost() {
        return !this.sortedHosts.isEmpty();
    }
}
