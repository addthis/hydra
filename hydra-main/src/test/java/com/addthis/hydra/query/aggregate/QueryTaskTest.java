package com.addthis.hydra.query.aggregate;

import java.util.concurrent.Semaphore;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.Query;
import com.addthis.meshy.service.file.FileReference;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.netty.channel.ChannelProgressivePromise;
import io.netty.util.concurrent.EventExecutor;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryTaskTest {

    private QueryTask queryTask;
    private Query query;

    private MeshSourceAggregator sourceAggregator;
    @Spy private QueryTaskSource taskSource0 = new QueryTaskSource(new QueryTaskSourceOption[0]);
    @Spy private QueryTaskSource taskSource1= new QueryTaskSource(new QueryTaskSourceOption[0]);
    @Spy private QueryTaskSource taskSource2= new QueryTaskSource(new QueryTaskSourceOption[0]);
    @Spy private QueryTaskSource taskSource3= new QueryTaskSource(new QueryTaskSourceOption[0]);
    @Mock private EventExecutor executor;

    @Before
    public void setup() throws Exception {
        query = new Query("jobid", new String[]{"path"}, new String[]{"ops"});

        MockitoAnnotations.initMocks(this);

        QueryTaskSource[] taskSources = new QueryTaskSource[]{taskSource0, taskSource1, taskSource2, taskSource3};
        for (QueryTaskSource x: taskSources) {
            stubSelectedSource(x);
        }

        MeshSourceAggregator underlying = new MeshSourceAggregator(taskSources, null, null, query);
        underlying.queryPromise = mock(ChannelProgressivePromise.class);
        underlying.consumer = mock(DataChannelOutput.class);
        underlying.channelWritable = true;
        underlying.executor = executor;
        sourceAggregator = spy(underlying);

        queryTask = new QueryTask(sourceAggregator);
    }

    private void stubSelectedSource(QueryTaskSource taskSource) {
        FileReference ref = new FileReference("name", 0, 0);
        QueryTaskSourceOption option = new QueryTaskSourceOption(ref, new Semaphore(3));
        when(taskSource.getSelectedSource()).thenReturn(option);
    }

    @Test
    public void basicSuccess() throws Exception {
        // stub each QueryTaskSource to return one bundle only
        for (QueryTaskSource x: sourceAggregator.taskSources) {
            // stub false return value twice: for taskSourceProvider then first iteration over task
            // sources in readFrame
            when(x.complete()).thenReturn(false).thenReturn(false).thenReturn(true);
            when(x.next()).thenReturn(mock(Bundle.class));
        }
        when(sourceAggregator.queryPromise.trySuccess()).thenReturn(true);

        queryTask.run();

        // verify 4 bundles processed and query promise success
        verify(sourceAggregator.consumer, times(4)).send(any(Bundle.class));
        verify(sourceAggregator.queryPromise).tryProgress(0, 4);
        verify(sourceAggregator.queryPromise).trySuccess();
    }

    @Test
    public void noActiveTaskSource() throws Exception {
        // stub all task sources to return null bundle, and flags indicating blockage
        for (QueryTaskSource x : sourceAggregator.taskSources) {
            when(x.next()).thenReturn(null);
            when(x.complete()).thenReturn(false);
            when(x.hasNoActiveSources()).thenReturn(true);
        }

        queryTask.run();

        // verify attempts to active each task source, and resubmission of the query task for execution
        verify(sourceAggregator, times(4)).tryActivateSource(any(QueryTaskSource.class));
        verify(sourceAggregator.executor).execute(queryTask);
    }

    @Test
    public void oneTask() throws Exception {
        // stub 3 empty task sources (as would be the case with the "tasks" query parameter)
        when(taskSource0.complete()).thenReturn(true);
        when(taskSource1.complete()).thenReturn(true);
        when(taskSource2.complete()).thenReturn(true);
        // stub one QueryTaskSource to return one bundle
        when(taskSource3.complete()).thenReturn(false).thenReturn(false).thenReturn(true);
        when(taskSource3.next()).thenReturn(mock(Bundle.class));
        when(sourceAggregator.queryPromise.trySuccess()).thenReturn(true);

        queryTask.run();

        // verify 1 bundle processed and query promise success
        verify(sourceAggregator.consumer).send(any(Bundle.class));
        verify(sourceAggregator.queryPromise).tryProgress(0, 1);
        verify(sourceAggregator.queryPromise).trySuccess();
    }
}