package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.AvailabilityDomain;
import com.addthis.hydra.job.spawn.HostLocationSummary;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.WithScore;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HostCandidateIteratorTest {
    private Spawn spawn;
    private SpawnBalancer bal;
    private HostManager hostManager;
    Map<HostState, Double> scoreMap = new HashMap<>();

   private String hostId1 = "hostId1";
   private String hostId2 = "hostId2";
   private String hostId3 = "hostId3";
   private String hostId4 = "hostId4";
   private String hostId5 = "hostId5";
   private String hostId6 = "hostId6";
   private String hostId7 = "hostId7";
   private String hostId8 = "hostId8";
   private String hostId9 = "hostId9";
   private HostState host1;
   private HostState host2;
   private HostState host3;
   private HostState host4;
   private HostState host5;
   private HostState host6;
   private HostState host7;
   private HostState host8;
   private HostState host9;

    @Before
    public void setup() {
        spawn = mock(Spawn.class);
        hostManager = mock(HostManager.class);
        bal = mock(SpawnBalancer.class);
        Whitebox.setInternalState(spawn, "hostManager", hostManager);
        when(spawn.getSpawnBalancer()).thenReturn(bal);        SpawnBalancerConfig c = new SpawnBalancerConfig();
        c.setSiblingWeight(40);
        when(bal.getConfig()).thenReturn(c);
        host1 = setUpHostState(hostId1, true, "a", "aa", "aaa", 0);
        host2 = setUpHostState(hostId2, true, "b", "bb", "bbb", 0);
        host3 = setUpHostState(hostId3, true, "a", "aa", "aab", 0);
        host4 = setUpHostState(hostId4, true, "a", "ab", "aba", 0);
        host5 = setUpHostState(hostId5, true, "a", "ab", "aba", 0);
        host6 = setUpHostState(hostId6, true, "c", "cc", "ccc", 0);

        when(bal.okToPutReplicaOnHost(any(), any())).thenReturn(true);
        HostLocationSummary summary = new HostLocationSummary();
        summary.updateHostLocationSummary(Arrays.asList(host1, host2, host3, host4, host5, host6));
        when(hostManager.getHostLocationSummary()).thenReturn(summary);
    }

    @Test
    public void highScoreHostsShouldReturnReplicas() {
        scoreMap.put(host1, 80d);
        scoreMap.put(host2, 80d);
        scoreMap.put(host3, 80d);
        scoreMap.put(host4, 80d);
        scoreMap.put(host5, 80d);
        scoreMap.put(host6, 80d);

        JobTask task = setupJobTask(hostId1);
        HostCandidateIterator hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task), scoreMap);
        List<String> newReplicas = hostCandidateIterator.getNewReplicaHosts(5, task);

        // Hosts 1 and 5 have the same HostLocation as other chosen hosts
        // They will not be chosen until all other options are exhausted
        assertTrue(newReplicas.size() == 5);
        assertTrue(hostId2.equals(newReplicas.get(0)));
        assertTrue(hostId6.equals(newReplicas.get(1)));
        assertTrue(hostId4.equals(newReplicas.get(2)));
        assertTrue(hostId3.equals(newReplicas.get(3)));
        assertTrue(hostId2.equals(newReplicas.get(4)));
    }

    @Test
    public void repeatHostLocationWhenNecessary() {
        scoreMap.put(host1, 30d);
        scoreMap.put(host2, 30d);
        scoreMap.put(host3, 30d);
        scoreMap.put(host4, 30d);
        scoreMap.put(host5, 30d);
        scoreMap.put(host6, 30d);

        JobTask task = setupJobTask(hostId1, hostId2, hostId6);
        HostCandidateIterator hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task), scoreMap);
        List<String> newReplicas = hostCandidateIterator.getNewReplicaHosts(5, task);
        assertTrue(hostId4.equals(newReplicas.get(0)));
        assertTrue(hostId3.equals(newReplicas.get(1)));
        assertTrue(hostId2.equals(newReplicas.get(2)));
        assertTrue(hostId6.equals(newReplicas.get(3)));
        assertTrue(hostId5.equals(newReplicas.get(4)));
    }

    @Test
    public void sortedHostsCreatedCorrectly() {
        scoreMap.put(host1, 20d);
        scoreMap.put(host2, 50d);
        scoreMap.put(host3, 40d);
        scoreMap.put(host4, 30d);
        scoreMap.put(host5, 10d);
        scoreMap.put(host6, 20d);
        // make tasks with hosts and replicas
        List<JobTask> tasks = new ArrayList<>();
        tasks.add(setupJobTask(hostId1, hostId2, hostId3));
        tasks.add(setupJobTask(hostId1, hostId2, hostId3));
        tasks.add(setupJobTask(hostId1, hostId2, hostId3));
        tasks.add(setupJobTask(hostId4, hostId5, hostId1));

        TreeSet<WithScore<HostState>> result =
                HostCandidateIterator.generateSortedHosts(hostManager, tasks, scoreMap, 40);
        List<WithScore<HostState>> resultList = new ArrayList<>(result);
        assertEquals(hostId6, resultList.get(0).element.getHostUuid());
        assertEquals(hostId5, resultList.get(1).element.getHostUuid());
        assertEquals(hostId4, resultList.get(2).element.getHostUuid());
        assertEquals(hostId3, resultList.get(3).element.getHostUuid());
        assertEquals(hostId2, resultList.get(4).element.getHostUuid());
        assertEquals(hostId1, resultList.get(5).element.getHostUuid());
        assertEquals(20d, resultList.get(0).score, 0);
        assertEquals(50d, resultList.get(1).score, 0);
        assertEquals(70d, resultList.get(2).score, 0);
        assertEquals(160d, resultList.get(3).score, 0);
        assertEquals(170d, resultList.get(4).score, 0);
        assertEquals(180d, resultList.get(5).score, 0);
    }

    @Test
    public void newReplicasShouldSpreadAcrossAd() {
        JobTask task = setupJobTask(hostId1);
        scoreMap.put(hostManager.getHostState(hostId1), 20d);
        scoreMap.put(hostManager.getHostState(hostId2), 50d);
        scoreMap.put(hostManager.getHostState(hostId3), 50d);
        scoreMap.put(hostManager.getHostState(hostId4), 30d);
        scoreMap.put(hostManager.getHostState(hostId5), 10d);
        scoreMap.put(hostManager.getHostState(hostId6), 20d);

        HostCandidateIterator hostCandidateIterator =
                new HostCandidateIterator(spawn, Arrays.asList(task), scoreMap);
        List<String> hostIdsToAdd = hostCandidateIterator.getNewReplicaHosts(5, task);
        assertTrue("Host candidate iterator should have hosts", !hostIdsToAdd.isEmpty());
        Iterator<String> iterator = hostIdsToAdd.iterator();
        assertTrue("Should choose HostLocation with min score and different datacenter",
                   iterator.next().equals(hostId6));
        assertTrue("Should choose HostLocation on different datacenter next",
                   iterator.next().equals(hostId2));
        assertTrue("Should choose Host with lower score on different rack next",
                   iterator.next().equals(hostId5));
        assertTrue("Should choose Host on different physical host next",
                   iterator.next().equals(hostId3));
        assertTrue("Should not choose Host in the same location if other hosts available",
                   iterator.next().equals(hostId6));
    }

    @Test
    public void emptyListForInvalidNumOfReplicas() {
        JobTask task = new JobTask(hostId1, 0, 0);
        HostCandidateIterator hostCandidateIterator =
                new HostCandidateIterator(spawn, Arrays.asList(task), new HashMap<>());
        assertTrue(hostCandidateIterator.getNewReplicaHosts(0, new JobTask()).isEmpty());
        assertTrue(hostCandidateIterator.getNewReplicaHosts(-1, new JobTask()).isEmpty());
    }

    @Test public void replicasShouldBeOnSeparateHostLocations() {
        host1 = setUpHostState(hostId1, true, "a", "aa", "aaa", 23);
        host2 = setUpHostState(hostId2, true, "a", "aa", "aab", 100);
        host3 = setUpHostState(hostId3, true, "a", "ab", "abb", 100);
        host4 = setUpHostState(hostId4, true, "b", "ba", "baa", 2);
        host5 = setUpHostState(hostId5, true, "b", "bb", "bba", 14);
        host6 = setUpHostState(hostId6, true, "b", "bc", "bca", 20);
        host7 = setUpHostState(hostId7, true, "c", "ca", "caa", 0);
        host8 = setUpHostState(hostId8, true, "c", "ca", "cab", 0);
        host9 = setUpHostState(hostId9, true, "c", "ca", "cac", 0);

        List<JobTask> tasks = new ArrayList<>();
        HostCandidateIterator impl = new HostCandidateIterator(spawn, tasks, scoreMap);
        JobTask task = setupJobTask(hostId9);
        List<String> newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId4), new HashSet<>(newReplicas));
        task = setupJobTask(hostId9, hostId4);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId1), new HashSet<>(newReplicas));
        task = setupJobTask(hostId9, hostId4, hostId1);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId5), new HashSet<>(newReplicas));


        host1 = setUpHostState(hostId1, true, "a", "aa", "aaa", 0);
        host2 = setUpHostState(hostId2, true, "a", "aa", "aab", 0);
        host3 = setUpHostState(hostId3, true, "a", "ab", "abb", 1);
        host4 = setUpHostState(hostId4, true, "b", "ba", "baa", 0);
        host5 = setUpHostState(hostId5, true, "b", "bb", "bba", 0);
        host6 = setUpHostState(hostId6, true, "b", "bc", "bca", 0);
        host7 = setUpHostState(hostId7, true, "c", "ca", "caa", 2);
        host8 = setUpHostState(hostId8, true, "c", "ca", "cab", 3);
        host9 = setUpHostState(hostId9, true, "c", "ca", "cac", 0);

        task = setupJobTask(hostId9, hostId4, hostId1, hostId5);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId3), new HashSet<>(newReplicas));
        task = setupJobTask(hostId9, hostId4, hostId1, hostId5, hostId3);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId7), new HashSet<>(newReplicas));

        host1 = setUpHostState(hostId1, true, "a", "aa", "aaa", 0);
        host2 = setUpHostState(hostId2, true, "a", "aa", "aab", 2);
        host3 = setUpHostState(hostId3, true, "a", "ab", "abb", 0);
        host4 = setUpHostState(hostId4, true, "b", "ba", "baa", 0);
        host5 = setUpHostState(hostId5, true, "b", "bb", "bba", 0);
        host6 = setUpHostState(hostId6, true, "b", "bc", "bca", 11);
        host7 = setUpHostState(hostId7, true, "c", "ca", "caa", 0);
        host8 = setUpHostState(hostId8, true, "c", "ca", "cab", 3);
        host9 = setUpHostState(hostId9, true, "c", "ca", "cac", 0);

        task = setupJobTask(hostId9, hostId4, hostId1, hostId5, hostId3, hostId7);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId6), new HashSet<>(newReplicas));
        task = setupJobTask(hostId9, hostId4, hostId1, hostId5, hostId3, hostId7, hostId6);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId2), new HashSet<>(newReplicas));
        task = setupJobTask(hostId9, hostId4, hostId1, hostId5, hostId3, hostId7, hostId6, hostId2);
        newReplicas = impl.getNewReplicaHosts(1, task, null, true);
        assertEquals(Sets.newHashSet(hostId8), new HashSet<>(newReplicas));
    }

    @Test public void chooseFarthestHostWithLowestScore() {
        JobTask task1 = setupJobTask(hostId1, hostId6);
        JobTask task2 = setupJobTask(hostId2, hostId4, hostId5, hostId6);
        Map<HostState, Double> scoreMap = new HashMap<>();
        scoreMap.put(host1, 20d);
        scoreMap.put(host2, 50d);
        scoreMap.put(host3, 50d);
        scoreMap.put(host4, 30d);
        scoreMap.put(host5, 10d);
        scoreMap.put(host6, 20d);

        HostCandidateIterator hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task1), scoreMap);
        List<String> hostIdsForTask1 = hostCandidateIterator.getNewReplicaHosts(2, task1);
        assertTrue(hostIdsForTask1.size() == 2);
        assertTrue(hostIdsForTask1.get(0).equals(hostId2));
        assertTrue(hostIdsForTask1.get(1).equals(hostId5));

        hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task2), scoreMap);
        List<String> hostIdsForTask2 = hostCandidateIterator.getNewReplicaHosts(1, task2);
        assertTrue(hostIdsForTask2.size() == 1);
        assertTrue(hostIdsForTask2.get(0).equals(hostId1));
    }

    @Test public void selectHostsBasedOnHostScore() {
        HostLocation location = new HostLocation("Unknown", "Unknown", "Unknown");
        host1.setHostLocation(location);
        host2.setHostLocation(location);
        host3.setHostLocation(location);
        host4.setHostLocation(location);
        host5.setHostLocation(location);
        host6.setHostLocation(location);
        hostManager.getHostLocationSummary()
                .updateHostLocationSummary(Arrays.asList(host1, host2, host3, host4, host5, host6));
        assertTrue(hostManager.getHostLocationSummary().getPriorityLevel() == AvailabilityDomain.NONE);

        JobTask task1 = setupJobTask(hostId1, hostId6);
        JobTask task2 = setupJobTask(hostId2, hostId4, hostId5, hostId6);
        Map<HostState, Double> scoreMap = new HashMap<>();
        scoreMap.put(host1, 20d);
        scoreMap.put(host2, 50d);
        scoreMap.put(host3, 50d);
        scoreMap.put(host4, 30d);
        scoreMap.put(host5, 10d);
        scoreMap.put(host6, 20d);

        HostCandidateIterator hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task1), scoreMap);
        List<String> replicasForTask1 = hostCandidateIterator.getNewReplicaHosts(5, task1);
        assertTrue(replicasForTask1.size() == 5);
        assertTrue(hostId5.equals(replicasForTask1.get(0)));
        assertTrue(hostId4.equals(replicasForTask1.get(1)));
        assertTrue(hostId2.equals(replicasForTask1.get(2)));
        assertTrue(hostId3.equals(replicasForTask1.get(3)));
        assertTrue(hostId5.equals(replicasForTask1.get(4)));
    }

    private HostState setUpHostState(String hostId, boolean isUp, String dc, String ra, String ph, double score) {
        HostState host = hostManager.getHostState(hostId);
        if (host == null) {
            host = new HostState(hostId);
        }
        host.setUp(isUp);
        host.setHostLocation(new HostLocation(dc, ra, ph));
        when(hostManager.getHostState(hostId)).thenReturn(host);
        scoreMap.put(host, score);
        return host;
    }

    private JobTask setupJobTask(String hostUUID, String... replicaUUIDs) {
        JobTask t = new JobTask();
        t.setHostUUID(hostUUID);
        List<JobTaskReplica> replicas = new ArrayList<>();
        for (String replicaUUID : replicaUUIDs) {
            replicas.add(new JobTaskReplica(replicaUUID, null, 0, 0));
        }
        t.setReplicas(replicas);
        return t;
    }
}
