package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.HostLocation;

public class HostCandidateIterator {
    List<PriorityQueue<HostAndScore>> orderedHeaps;
    PriorityQueue<HostAndScore> currentRound;
    private static HostManager hostManager;
    private static SpawnBalancer balancer;
    private static Comparator comparator;

    HostCandidateIterator(HostManager hostManager, SpawnBalancer spawnBalancer, Comparator comparator) {
        this.hostManager = hostManager;
        this.balancer = spawnBalancer;
        this.comparator = comparator;
        currentRound = new PriorityQueue<>(comparator);
        orderedHeaps = new ArrayList<>();
    }

    public void generateHostCandidateIterator (Map<String, Double> scoreMap, JobTask task) {
        Map<HostLocation, PriorityQueue<HostAndScore>> scoreHeapByLocation = new HashMap<>();
        for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
            HostState hostState = hostManager.getHostState(entry.getKey());
            if(hostState == null ||
               !(balancer.okToPutReplicaOnHost(hostState, task) && hostState.canMirrorTasks())) {
                continue;
            }
            HostLocation location = hostState.getHostLocation();
            Double score = entry.getValue();
            PriorityQueue<HostAndScore> scoreHeap = scoreHeapByLocation.getOrDefault(
                    location, new PriorityQueue<>(1, comparator));
            scoreHeap.add(new HostAndScore(hostState, score));
            // Should replace with updated heap if a mapping already exists
            scoreHeapByLocation.put(location, scoreHeap);
        }

        orderedHeaps.addAll(scoreHeapByLocation.values());
    }

    public HostState getNext() {
        if (currentRound.isEmpty()) {
            currentRound = getCurrentRound();
        }
        HostAndScore nextHostAndScore = currentRound.poll();
        return nextHostAndScore.host;
    }

    public boolean hasNextHost() {
        return !currentRound.isEmpty() || hasNextRound();
    }

    private boolean hasNextRound() {
        for(PriorityQueue<HostAndScore> heap : orderedHeaps) {
            if(!heap.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public PriorityQueue<HostAndScore> getCurrentRound() {
        PriorityQueue<HostAndScore> currentRound = new PriorityQueue<>(comparator);
        for (PriorityQueue<HostAndScore> heap : orderedHeaps) {
            if (!heap.isEmpty()) {
                HostAndScore hs = heap.poll(); // pick the highest from each heap
                currentRound.add(hs);
                // move to the end of the heap; reuse after all hosts have been selected
                heap.add(new HostAndScore(hs.host, hs.score + 1));
            }
        }
        return currentRound;
    }
}
