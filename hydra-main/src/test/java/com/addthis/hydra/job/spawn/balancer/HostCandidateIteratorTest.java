package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.HostLocationSummary;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.WithScore;

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

    private String hostId1 = "hostId1";
    private String hostId2 = "hostId2";
    private String hostId3 = "hostId3";
    private String hostId4 = "hostId4";
    private String hostId5 = "hostId5";
    private String hostId6 = "hostId6";

    private HostState host1 = setUpHostState(hostId1, true,"a", "aa", "aaa");
    private HostState host2 = setUpHostState(hostId2, true,"b", "bb", "bbb");
    private HostState host3 = setUpHostState(hostId3, true,"a", "aa", "aab");
    private HostState host4 = setUpHostState(hostId4, true, "a", "ab", "aba");
    private HostState host5 = setUpHostState(hostId5, true, "a", "ab", "aba");
    private HostState host6 = setUpHostState(hostId6, true, "c", "cc", "ccc");

    @Before
    public void setup() {
        spawn = mock(Spawn.class);
        hostManager = mock(HostManager.class);
        bal = mock(SpawnBalancer.class);
        Whitebox.setInternalState(spawn, "hostManager", hostManager);
        when(spawn.getSpawnBalancer()).thenReturn(bal);
        when(hostManager.getHostState(hostId1)).thenReturn(host1);
        when(hostManager.getHostState(hostId2)).thenReturn(host2);
        when(hostManager.getHostState(hostId3)).thenReturn(host3);
        when(hostManager.getHostState(hostId4)).thenReturn(host4);
        when(hostManager.getHostState(hostId5)).thenReturn(host5);
        when(hostManager.getHostState(hostId6)).thenReturn(host6);

        when(hostManager.getHostLocationForHost(hostId1)).thenReturn(host1.getHostLocation());
        when(hostManager.getHostLocationForHost(hostId2)).thenReturn(host2.getHostLocation());
        when(hostManager.getHostLocationForHost(hostId3)).thenReturn(host3.getHostLocation());
        when(hostManager.getHostLocationForHost(hostId4)).thenReturn(host4.getHostLocation());
        when(hostManager.getHostLocationForHost(hostId5)).thenReturn(host5.getHostLocation());
        when(hostManager.getHostLocationForHost(hostId6)).thenReturn(host6.getHostLocation());

        SpawnBalancerConfig c = new SpawnBalancerConfig();
        c.setSiblingWeight(40);
        when(bal.getConfig()).thenReturn(c);

        when(bal.okToPutReplicaOnHost(any(), any())).thenReturn(true);
        HostLocationSummary summary = new HostLocationSummary();
        summary.updateHostLocationSummary(Arrays.asList(host1, host2, host3, host4, host5, host6));
        when(hostManager.getHostLocationSummary()).thenReturn(summary);
    }

    @Test
    public void highScoreHostsShouldReturnReplicas() {
        Map<HostState, Double> scoreMap = new HashMap<>();
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
        Map<HostState, Double> scoreMap = new HashMap<>();
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
        // make scoremap
        Map<HostState, Double> scoreMap = new HashMap<>();
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
        JobTask task = new JobTask(hostId1, 0, 0);
        Map<HostState, Double> scoreMap = new HashMap<>();
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

    @Test
    public void chooseFarthestHostWithLowestScore() {
        JobTask task1 = setupJobTask(hostId1, hostId6);
        JobTask task2 = setupJobTask(hostId2, hostId4, hostId5, hostId6);
        Map<HostState, Double> scoreMap = new HashMap<>();
        scoreMap.put(host1, 20d);
        scoreMap.put(host2, 50d);
        scoreMap.put(host3, 50d);
        scoreMap.put(host4, 30d);
        scoreMap.put(host5, 10d);
        scoreMap.put(host6, 20d);

        HostCandidateIterator hostCandidateIterator =
                new HostCandidateIterator(spawn, Arrays.asList(task1), scoreMap);
        List<String> hostIdsForTask1 = hostCandidateIterator.getNewReplicaHosts(2, task1);
        assertTrue(hostIdsForTask1.size() == 2);
        assertTrue(hostIdsForTask1.get(0).equals(hostId2));
        assertTrue(hostIdsForTask1.get(1).equals(hostId5));

        hostCandidateIterator = new HostCandidateIterator(spawn, Arrays.asList(task2), scoreMap);
        List<String> hostIdsForTask2 = hostCandidateIterator.getNewReplicaHosts(1, task2);
        assertTrue(hostIdsForTask2.size() == 1);
        assertTrue(hostIdsForTask2.get(0).equals(hostId1));
    }

    private HostState setUpHostState(String hostId, boolean isUp, String dc, String ra, String ph) {
        HostState host = new HostState(hostId);
        host.setUp(isUp);
        host.setHostLocation(new HostLocation(dc, ra, ph));
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
