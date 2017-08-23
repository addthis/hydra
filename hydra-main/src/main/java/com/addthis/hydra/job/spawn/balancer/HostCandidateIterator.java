package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.minion.Zone;

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

    public void generateHostCandidateIterator (Map<String, Double> scoreMap,
                                               JobTask task) {
        Map<Zone, PriorityQueue<HostAndScore>> scoreHeapByZone = new HashMap<>();
        for (Map.Entry<String, Double> entry : scoreMap.entrySet()) {
            HostState hostState = hostManager.getHostState(entry.getKey());
            if (!balancer.okToPutReplicaOnHost(hostState, task)) continue;
            if (!hostState.canMirrorTasks()) continue;
            Zone zone = hostState.getZone();
            Double score = entry.getValue();
            PriorityQueue<HostAndScore> scoreHeap = scoreHeapByZone.getOrDefault(
                    zone, new PriorityQueue<>(1, comparator));
            scoreHeap.add(new HostAndScore(hostState, score));
            scoreHeapByZone.putIfAbsent(zone, scoreHeap);
        }

        PriorityQueue<HostAndScore> taskHeapByZone =
                scoreHeapByZone.getOrDefault(hostManager.getHostState(task.getHostUUID()).getZone(), new PriorityQueue<>(comparator));

        if(scoreHeapByZone.get(hostManager.getHostState(task.getHostUUID()).getZone()) != null) {
            scoreHeapByZone.remove(hostManager.getHostState(task.getHostUUID()).getZone());
        }
        List<JobTaskReplica> replicas = task.getReplicas();
        List<PriorityQueue<HostAndScore>> replicaHeaps = new ArrayList<>();
        for (JobTaskReplica replica : replicas) {
            PriorityQueue<HostAndScore> replicaHeap =
                    scoreHeapByZone.getOrDefault(hostManager.getHostState(replica.getHostUUID()).getZone(), new PriorityQueue<>(comparator));
            replicaHeaps.add(replicaHeap);
            scoreHeapByZone.remove(hostManager.getHostState(replica.getHostUUID()).getZone());
        }

        for (Map.Entry<Zone, PriorityQueue<HostAndScore>> entry : scoreHeapByZone.entrySet()) {
            orderedHeaps.add(entry.getValue());
        }
        orderedHeaps.add(taskHeapByZone);
        orderedHeaps.addAll(replicaHeaps);
    }

    public HostState getNext() {
        if (currentRound.size() == 0) {
            currentRound = getCurrentRound();
        }
        HostAndScore nextHostAndScore = currentRound.poll();
        // add to the end of the heap, by adding 1 to its score
        return nextHostAndScore.host;
    }

    public PriorityQueue<HostAndScore> getCurrentRound() {
        // Should you use a NEW currentRound Priority queue???
        currentRound = new PriorityQueue<>(comparator);
        for (PriorityQueue<HostAndScore> heap : orderedHeaps) {
            if (heap.size() == 0) continue;
            HostAndScore hs = heap.poll(); // pick the highest from each heap
            currentRound.add(hs);
            // move to the end of the heap; reuse after all hosts have been selected
            heap.add(new HostAndScore(hs.host, hs.score + 1));
        }
        return currentRound;
    }

}
