package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
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
    private Comparator<HostAndScore> comparator;
    private Map<HostLocation, PriorityQueue<HostAndScore>> scoreHeapByLocation;
    private List<HostState> orderedHosts;

    HostCandidateIterator(HostManager hostManager, SpawnBalancer spawnBalancer, Comparator comparator) {
        this.hostManager = hostManager;
        this.balancer = spawnBalancer;
        this.comparator = comparator;
        scoreHeapByLocation = new HashMap<>();
        orderedHosts = new ArrayList<>();
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
        List<HostLocation> hostLocations = sortHostLocations(task);
        orderedHosts = orderHostsByLocationAndScore(hostLocations);
    }

    /**
     * Return a list of HostLocation arranged in order of zone preference
     * HostLocation with task and replicas have lower preference
     * @param task
     * @return
     */
    private List<HostLocation> sortHostLocations(JobTask task) {
        // Sort HostLocation(s) using Host score
        Comparator<Map.Entry<HostLocation, PriorityQueue<HostAndScore>>> sortComparator;
        Comparator<Map.Entry<HostLocation, PriorityQueue<HostAndScore>>> scoreComparator;
        scoreComparator = Comparator.comparingInt(entry -> entry.getKey().assignScoreByHostLocation(hostManager.getHostState(task.getHostUUID()).getHostLocation()));
        sortComparator = scoreComparator.thenComparingDouble(entry -> entry.getValue().peek().score);
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
     * Arrange hosts in order of HostLocation preference, then score
     * @param hostLocations
     * @return
     */
    private List<HostState> orderHostsByLocationAndScore(List<HostLocation> hostLocations) {
        List<HostState> hostStates = new ArrayList<>();
        for(HostLocation location : hostLocations) {
            PriorityQueue<HostAndScore> scoreHeap = scoreHeapByLocation.get(location);
            while(scoreHeap != null && !scoreHeap.isEmpty()) {
                hostStates.add(scoreHeap.poll().host);
            }
        }
        return hostStates;
    }

    /**
     * Return a host chosen in order of zone preference, then score
     * @return
     */
    public HostState getNextHost() {
        HostState nextHost = orderedHosts.get(0);
        orderedHosts.remove(nextHost);
        orderedHosts.add(nextHost);
        return nextHost;
    }

    public boolean hasNextHost() {
        return !orderedHosts.isEmpty();
    }
}
