package com.addthis.hydra.job.spawn.balancer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.AvailabilityDomain;
import com.addthis.hydra.job.spawn.HostLocationSummary;
import com.addthis.hydra.job.spawn.HostManager;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.minion.HostLocation;
import com.addthis.hydra.util.ZkCodecStartUtil;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HostCandidateIteratorTest extends ZkCodecStartUtil {

    private Spawn spawn;
    private SpawnBalancer bal;
    private HostManager hostManager;

    String hostId1 = "hostId1";
    String hostId2 = "hostId2";
    String hostId3 = "hostId3";
    String hostId4 = "hostId4";
    String hostId5 = "hostId5";
    String hostId6 = "hostId6";

    @Before
    public void setup() {
        spawn = mock(Spawn.class);
        hostManager = mock(HostManager.class);
        bal = mock(SpawnBalancer.class);
        Whitebox.setInternalState(spawn, "hostManager", hostManager);
        when(spawn.getSpawnBalancer()).thenReturn(bal);
        when(hostManager.getHostState(hostId1)).thenReturn(setUpHostState(hostId1, true,"a", "aa", "aaa"));
        when(hostManager.getHostState(hostId2)).thenReturn(setUpHostState(hostId2, true,"b", "bb", "bbb"));
        when(hostManager.getHostState(hostId3)).thenReturn(setUpHostState(hostId3, true,"a", "aa", "aab"));
        when(hostManager.getHostState(hostId4)).thenReturn(setUpHostState(hostId4, true,"a", "ab", "aba"));
        when(hostManager.getHostState(hostId5)).thenReturn(setUpHostState(hostId5, true,"a", "ab", "aba"));
        when(hostManager.getHostState(hostId6)).thenReturn(setUpHostState(hostId6, true,"c", "cc", "ccc"));
        SpawnBalancerConfig c = new SpawnBalancerConfig();
        c.setSiblingWeight(40);
        when(bal.getConfig()).thenReturn(c);

        when(bal.okToPutReplicaOnHost(any(), any())).thenReturn(true);
        HostLocationSummary summary = mock(HostLocationSummary.class);
        when(hostManager.getHostLocationSummary()).thenReturn(summary);
        when(summary.getPriorityLevel()).thenReturn(AvailabilityDomain.DATACENTER);



    }

    @Test
    public void testHostCandidateIterator() throws Exception {
        Job job = mock(Job.class);
        JobTask task1 = new JobTask("hostId1", 0, 0);
        when(job.getCopyOfTasks()).thenReturn(Arrays.asList(task1));

        Map<HostState, Double> scoreMap = new HashMap<>();
        scoreMap.put(hostManager.getHostState(hostId1), 10d);
        scoreMap.put(hostManager.getHostState(hostId2), 20d);

        for(JobTask task : job.getCopyOfTasks()) {
            // Use a dummy value of 25 for taskScoreIncrement
            HostCandidateIterator hostCandidateIterator =
                    new HostCandidateIterator(spawn, job.getCopyOfTasks(), scoreMap);
            List<String> hostIdsToAdd = hostCandidateIterator.getNewReplicaHosts(5, task);
            assertTrue("Host candidate iterator should have hosts", !hostIdsToAdd.isEmpty());

            Iterator<String> iterator = hostIdsToAdd.iterator();
            assertTrue("Should choose HostLocation with min score and different datacenter",
                       iterator.next().equals("hostId6"));
            assertTrue("Should choose HostLocation on different datacenter next",
                       iterator.next().equals("hostId2"));
            assertTrue("Should choose Host with lower score on different rack next",
                       iterator.next().equals("hostId5"));
            assertTrue("Should choose Host on different physical host next",
                       iterator.next().equals("hostId3"));
            assertTrue("Should not choose Host in the same location if other hosts available",
                       iterator.next().equals("hostId2"));
        }
    }

    private HostState setUpHostState(String hostId, boolean isUp, String dc, String ra, String ph) {
        HostState host = new HostState(hostId);
        host.setUp(isUp);
        host.setHostLocation(new HostLocation(dc, ra, ph));
        return host;
    }
}
