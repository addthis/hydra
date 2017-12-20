package com.addthis.hydra.job.spawn.balancer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {

//    private static final Comparator<HostAndScore> hostAndScoreComparator = Comparator.comparingDouble(has -> has.score);

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

    public HostCandidateIterator(
            HostManager hostManager,
            SpawnBalancer balancer,
            IJob job,
            int taskScoreIncrement) {
        this.hostManager = hostManager;
        this.balancer = balancer;
        this.sortedHosts = new TreeSet<>(hostAndScoreComparator);
        this.taskScoreIncrement = taskScoreIncrement;

        Map<String, Double> scoreMap = balancer.generateTaskCountHostScoreMap(job);
        for (JobTask task : job.getCopyOfTasks()) {
            for (String replicaHostId : task.getAllTaskHosts()) {
                Double score = scoreMap.get(replicaHostId);
                scoreMap.put(replicaHostId, score + this.taskScoreIncrement);
            }
        }
        // create the sortedHosts priority queue which will be used for picking candidate hosts
        for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
            HostState host = hostManager.getHostState(entry.getKey());
            if (host != null) {
                this.sortedHosts.add(new HostAndScore(host, entry.getValue()));
            }
        }
    }

    /**
     * Return a host chosen in order of zone preference, then score
     */
    public List<String> getNewReplicaHosts(int replicas, JobTask task, @Nullable String excludeHost) {
        Collection<HostLocation> locations = new HashSet<>();
        // get current host locations used by live/replicas
        for (String replicaHostId : task.getAllTaskHosts()) {
            locations.add(hostManager.getHostState(replicaHostId).getHostLocation());
        }

        List<String> chosenHostIds = new ArrayList<>(replicas);
        // create each replica
        for (int i = 0; i < replicas; i++) {
            int minScoreSoFar = Integer.MAX_VALUE;
            HostAndScore bestHost = null;
            for (HostAndScore hostAndScore : sortedHosts) {
                HostState host = hostAndScore.host;
                boolean excluded = host.getHostUuid().equals(excludeHost);
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
                this.sortedHosts.add(new HostAndScore(bestHost.host, bestHost.score + this.taskScoreIncrement));
                locations.add(bestHost.host.getHostLocation());
                chosenHostIds.add(bestHost.host.getHostUuid());
            }
        }
        return chosenHostIds;
    }

    public List<String> getNewReplicaHosts(int replicas, JobTask task) {
        return getNewReplicaHosts(replicas, task, null);
    }

    public List<String> getNewReplicaHosts(JobTask task, @Nullable String excludeHost) {
        return getNewReplicaHosts(1, task, excludeHost);
    }
}