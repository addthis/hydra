package com.addthis.hydra.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.LessFiles;

import com.addthis.bark.ZkUtil;
import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.hydra.data.util.DateUtil;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.source.SimpleMark;
import com.addthis.hydra.task.source.TaskDataSource;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.curator.framework.CuratorFramework;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.kafka.consumer.BundleWrapper.bundleQueueEndMarker;
import static com.addthis.hydra.kafka.consumer.MessageWrapper.messageQueueEndMarker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class KafkaSource extends TaskDataSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    @JsonProperty(required = true)
    private String zookeeper;
    @JsonProperty(required = true)
    private String topic;
    @JsonProperty
    private String inputBundleFormatType = KafkaByteDecoder.KafkaByteDecoderType.BUNDLE.toString();
    @JsonProperty
    private String startDate;
    @JsonProperty
    private String dateFormat = "YYMMdd";
    @JsonProperty
    private String markDir = "marks";

    @JsonProperty
    private int fetchThreads = 1;
    @JsonProperty
    private int decodeThreads = 1;
    @JsonProperty
    private int queueSize = 10000;
    @JsonProperty
    private int seedBrokers = 3;

    @JsonProperty
    private TaskRunConfig config;
    @JsonProperty
    private int pollRetries = 180;

    PageDB<SimpleMark> markDb;
    AtomicBoolean running;
    LinkedBlockingQueue<MessageWrapper> messageQueue;
    LinkedBlockingQueue<BundleWrapper> bundleQueue;
    CuratorFramework zkClient;
    final ConcurrentMap<String, Long> sourceOffsets = new ConcurrentHashMap<>();

    private ExecutorService fetchExecutor;
    private ExecutorService decodeExecutor;

    @Override
    public Bundle next() throws DataChannelError {
        if (!running.get()) {
            return null;
        }
        BundleWrapper bundle;
        try {
            bundle = bundleQueue.poll(pollRetries, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // reset interrupt status
            Thread.currentThread().interrupt();
            // fine to leave bundles on the queue
            return null;
        }
        if (bundle == null) {
            throw new DataChannelError(
                    "giving up on kafka source next() after waiting: " + pollRetries + " seconds");
        }
        if (bundle == bundleQueueEndMarker) {
            // add back end-marker in case someone continues calling peek/next on the source
            bundleQueue.add(bundleQueueEndMarker);
            return null;
        }
        sourceOffsets.put(bundle.sourceIdentifier, bundle.offset);
        return bundle.bundle;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        BundleWrapper bundle = null;
        int retries = 0;
        while (bundle == null && retries < pollRetries) {
            bundle = bundleQueue.peek();
            // seemingly no better option than sleeping here - blocking queue doesn't have a
            // blocking peek, but
            // still want to maintain source.peek() == source.next() guarantees
            if (bundle == null) {
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
            }
            retries++;
        }
        if (bundle == null) {
            throw new DataChannelError(
                    "giving up on kafka source peek() after retrying: " + retries);
        }
        if (bundle == bundleQueueEndMarker) {
            return null;
        }
        return bundle.bundle;
    }

    @Override
    public void close() {
        running.set(false);
        fetchExecutor.shutdown();
        decodeExecutor.shutdown();
        for (Map.Entry<String, Long> sourceOffset : sourceOffsets.entrySet()) {
            SimpleMark mark = new SimpleMark();
            mark.set(String.valueOf(sourceOffset.getValue()), sourceOffset.getValue());
            mark.setEnd(false);
            this.markDb.put(new DBKey(0, sourceOffset.getKey()), mark);
            log.info("updating mark db, source: {}, index: {}", sourceOffset.getKey(), sourceOffset.getValue());
        }
        this.markDb.close();
        this.zkClient.close();
    }

    @Override
    public void init() {
        try {
            this.messageQueue = new LinkedBlockingQueue<>(queueSize);
            this.bundleQueue = new LinkedBlockingQueue<>(queueSize);
            this.markDb = new PageDB<>(LessFiles.initDirectory(markDir), SimpleMark.class, 100, 100);
            // move to init method
            this.fetchExecutor = MoreExecutors.getExitingExecutorService(
                    new ThreadPoolExecutor(fetchThreads, fetchThreads,
                            0l, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(),
                            new ThreadFactoryBuilder().setNameFormat("source-kafka-fetch-%d").build())
            );
            this.decodeExecutor = MoreExecutors.getExitingExecutorService(
                    new ThreadPoolExecutor(decodeThreads, decodeThreads,
                            0l, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(),
                            new ThreadFactoryBuilder().setNameFormat("source-kafka-decode-%d").build())
            );
            this.running = new AtomicBoolean(true);
            final DateTime startTime = (startDate != null) ? DateUtil.getDateTime(dateFormat, startDate) : null;

            zkClient = ZkUtil.makeStandardClient(zookeeper, false);
            TopicMetadata metadata = ConsumerUtils.getTopicMetadata(zkClient, seedBrokers, topic);

            final Integer[] shards = config.calcShardList(metadata.partitionsMetadata().size());
            final CountDownLatch fetchLatch = new CountDownLatch(shards.length);
            List<FetchTask> sortedConsumers = new ArrayList<>();
            for (final int shard : shards) {
                final PartitionMetadata partition = metadata.partitionsMetadata().get(shard);
                FetchTask fetcher = new FetchTask(this, fetchLatch, topic, partition, startTime);
                fetchExecutor.execute(fetcher);
            }
            fetchExecutor.submit(new MarkEndTask<>(fetchLatch, running, messageQueue, messageQueueEndMarker));

            final ListBundleFormat format = new ListBundleFormat();
            final CountDownLatch decodeLatch = new CountDownLatch(decodeThreads);
            Runnable decoder = new DecodeTask(decodeLatch, format, running, messageQueue, bundleQueue);
            for (int i = 0; i < decodeThreads; i++) {
                decodeExecutor.execute(decoder);
            }
            decodeExecutor.submit(new MarkEndTask<>(decodeLatch, running, bundleQueue, bundleQueueEndMarker));
        } catch (Exception ex) {
            log.error("Error initializing kafka source: ", ex);
            throw new RuntimeException(ex);
        }
    }

    // Put onto linked blocking queue, giving up (via exception) if running becomes false (interrupts are ignored in favor of the running flag).
    // Uses an exception rather than returning boolean since checking for return status was a huge mess (e.g. had to keep track of how far
    // your iterator got in the message set when retrying).
    static <E> void putWhileRunning(BlockingQueue<E> queue, E value, AtomicBoolean running) {
        boolean offered = false;
        while (!offered) {
            if (!running.get()) {
                throw BenignKafkaException.INSTANCE;
            }
            try {
                offered = queue.offer(value, 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }
}