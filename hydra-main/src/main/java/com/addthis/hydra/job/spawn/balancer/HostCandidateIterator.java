package com.addthis.hydra.job.spawn.balancer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {
    private static HostManager hostManager;
    private static SpawnBalancer balancer;

    private Comparator comparator;
    private List<HostLocation> hostLocations;
    private Map<HostLocation, PriorityQueue<HostAndScore>> scoreHeapByLocation;
    private int numHostsAccessed;

    HostCandidateIterator(HostManager hostManager, SpawnBalancer spawnBalancer, Comparator comparator) {
        this.hostManager = hostManager;
        this.balancer = spawnBalancer;
        this.comparator = comparator;
        scoreHeapByLocation = new HashMap<>();
        numHostsAccessed = 0;
    }

    /**
     * Generate a mapping from HostLocation to hosts, ordered by the host score
     * @param scoreMap
     * @param task
     */
    public void generateHostCandidateIterator(Map<String, Double> scoreMap, JobTask task) {
        for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
            HostState hostState = hostManager.getHostState(entry.getKey());
            if (hostState == null ||
                !(balancer.okToPutReplicaOnHost(hostState, task) && hostState.canMirrorTasks())) {
                continue;
            }
            HostLocation location = hostState.getHostLocation();
            Double score = entry.getValue();
            PriorityQueue<HostAndScore> scoreHeap =
                    scoreHeapByLocation.getOrDefault(location, new PriorityQueue<>(1, comparator));
            scoreHeap.add(new HostAndScore(hostState, score));
            // Should replace with updated heap if a mapping already exists
            scoreHeapByLocation.put(location, scoreHeap);
        }
        hostLocations = arrangeHostLocations(task);
    }

    /**
     * Return a list of HostLocation arranged in order of zone preference
     * HostLocation with task and replicas have lower preference
     * @param task
     * @return
     */
    private List<HostLocation> arrangeHostLocations(JobTask task) {
        // Sort HostLocation(s) using Host score
        Comparator<Map.Entry<HostLocation, PriorityQueue<HostAndScore>>> sortComparator;
        sortComparator = Comparator.comparing(entry -> entry.getKey().compareTo(hostManager.getHostState(task.getHostUUID()).getHostLocation()));
        sortComparator.thenComparingDouble(entry -> entry.getValue().peek().score);
        List<HostLocation> hostLocationList = scoreHeapByLocation.entrySet()
                                                                 .stream()
                                                                 .sorted(sortComparator)
                                                                 .map(entry -> entry.getKey())
                                                                 .collect(Collectors.toList());

        Map<HostLocation, Long> replicasByHostLocation = task.getAllReplicas()
                                                             .stream()
                                                             .collect(Collectors.groupingBy(
                                                                     jobTaskReplica -> hostManager.getHostState(jobTaskReplica.getHostUUID()).getHostLocation(),
                                                                     Collectors.counting()));
        List<HostLocation> sortedList = replicasByHostLocation.entrySet()
                                                              .stream()
                                                              .sorted(Comparator.comparingLong(entry -> entry.getValue()))
                                                              .map(entry-> entry.getKey())
                                                              .collect(Collectors.toList());
        // Move task and replica locations to the end of the list
        HostLocation taskLocation = hostManager.getHostState(task.getHostUUID()).getHostLocation();
        hostLocationList.removeAll(sortedList);
        hostLocationList.remove(taskLocation);
        hostLocationList.addAll(sortedList);
        hostLocationList.add(taskLocation);
        return hostLocationList;
    }

    /**
     * Return a host chosen in order of zone preference or <tt>null</tt> if no host exists
     * @return
     */
    public HostState getNextHost() {
        for(HostLocation location : hostLocations) {
            PriorityQueue<HostAndScore> scoreHeap = scoreHeapByLocation.get(location);
            if(scoreHeap != null && !scoreHeap.isEmpty()) {
                HostAndScore hostAndScore = scoreHeap.poll();
                numHostsAccessed++;
                if(scoreHeap.isEmpty() || numHostsAccessed == scoreHeap.size() + 1) {
                    // Move this location to the end of the list
                    // At this point all hosts from this location have been chosen once
                    hostLocations.remove(location);
                    hostLocations.add(location);
                    numHostsAccessed = 0;
                }
                // Add 1 to the host's score and move to the end of the queue
                scoreHeap.add(new HostAndScore(hostAndScore.host, hostAndScore.score + 1));
                scoreHeapByLocation.put(location, scoreHeap);
                return hostAndScore.host;
            }
        }
        return null;
    }

    public boolean hasNextHost() {
        return !scoreHeapByLocation.isEmpty();
    }
}
