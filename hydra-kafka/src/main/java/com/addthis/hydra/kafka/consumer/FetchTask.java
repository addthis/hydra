package com.addthis.hydra.kafka.consumer;

import java.net.URI;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.bark.StringSerializer;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.task.source.SimpleMark;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.data.io.kafka.KafkaSource.putWhileRunning;
import kafka.api.FetchRequest;
import kafka.common.InvalidMessageSizeException;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZkUtils;

class FetchTask implements Runnable, Comparable {

    private static final Logger log = LoggerFactory.getLogger(FetchTask.class);

    private static final int fetchSize = Parameter.intValue("hydra.kafka.fetchSize", 1048576);
    private static final int timeout = Parameter.intValue("hydra.kafka.timeout", 10000);

    private KafkaSource kafkaSource;
    private final CountDownLatch fetchLatch;
    private final CuratorFramework zkClient;
    private final InjectedBrokerInfo brokerInfo;
    private final int partition;
    private final DateTime startTime;

    public FetchTask(KafkaSource kafkaSource, CountDownLatch fetchLatch, CuratorFramework zkClient, InjectedBrokerInfo brokerInfo, int partition, DateTime startTime) {
        this.kafkaSource = kafkaSource;
        this.fetchLatch = fetchLatch;
        this.zkClient = zkClient;
        this.brokerInfo = brokerInfo;
        this.partition = partition;
        this.startTime = startTime;
    }

    @Override
    public void run() {
        consume(kafkaSource.running, fetchLatch, zkClient, brokerInfo, partition, kafkaSource.markDb, startTime, kafkaSource.messageQueue, kafkaSource.sourceOffsets);
    }

    private static long earliestOffsetAvailable(SimpleConsumer consumer, String topic, int partition) {
        return consumer.getOffsetsBefore(topic, partition, -2, 1)[0];
    }

    private static long latestOffsetAvailable(SimpleConsumer consumer, String topic, int partition) {
        return consumer.getOffsetsBefore(topic, partition, -1, 1)[0];
    }

    private static void consume(AtomicBoolean running, CountDownLatch latch, CuratorFramework zkClient,
            InjectedBrokerInfo brokerInfo, int partition, PageDB<SimpleMark> markDb, DateTime startTime,
            LinkedBlockingQueue<MessageWrapper> messageQueue, ConcurrentMap<String, Long> sourceOffsets) {
        try {
            if (!running.get()) {
                return;
            }
            // initialize consumer and offsets
            final String topic = brokerInfo.getTopic();
            final int brokerId = brokerInfo.getBrokerId();
            final URI brokerURI = getBrokerURI(zkClient, brokerId);
            final SimpleConsumer consumer = new SimpleConsumer(brokerURI.getHost(), brokerURI.getPort(), timeout, fetchSize);
            String sourceIdentifier = topic + "-" + brokerId + "-" + partition;
            final long endOffset = latestOffsetAvailable(consumer, topic, partition);
            final SimpleMark previousMark = markDb.get(new DBKey(0, sourceIdentifier));
            long offset = -1;
            if (previousMark != null) {
                offset = previousMark.getIndex();
            } else if (startTime != null) {
                long[] offsets = consumer.getOffsetsBefore(topic, partition, startTime.getMillis(), 1);
                if (offsets.length == 1) {
                    log.info("no previous mark for host: {}, partition: {}, starting from offset: {}, closest to: {}", consumer.host(), partition, offsets[0], startTime);
                    offset = offsets[0];
                }
            }
            if (offset == -1) {
                log.info("no previous mark for host: {}:{}, topic: {}, partition: {}, no offsets available for startTime: {}, starting from earliest", consumer.host(), consumer.port(), topic, partition, startTime);
                offset = earliestOffsetAvailable(consumer, topic, partition);
            } else if(offset > endOffset) {
                log.warn("initial offset for: {}:{}, topic: {}, partition: {} is beyond latest, {} > {}; kafka data was either wiped (resetting offsets) or corrupted - skipping " +
                         "ahead to offset {} to recover consuming from latest", consumer.host(), consumer.port(), topic, partition, offset, endOffset, endOffset);
                offset = endOffset;
                // Offsets are normally updated when the bundles are consumed from source.next() - since we wont be fetching any bundles to be consumed, we need
                // to update offset map (that gets persisted to marks) here.
                // The sourceOffsets map probably *should not* be modified anywhere else outside of next().
                sourceOffsets.put(sourceIdentifier, offset);
            }
            // fetch from broker, add to queue (decoder threads will process queue in parallel)
            while (running.get() && (offset < endOffset)) {
                ByteBufferMessageSet messageSet = null;
                try {
                    messageSet = consumer.fetch(new FetchRequest(topic, partition, offset, fetchSize));
                    // calling iterator() to check if message set returned is valid - sometimes fetch doesnt throw an exception, but iterator does
                    messageSet.iterator();
                // clamp out-of-range offsets
                } catch (OffsetOutOfRangeException e) {
                    messageSet = null;
                    long earliestOffset = earliestOffsetAvailable(consumer, topic, partition);
                    if (offset < earliestOffset) {
                        log.error("forwarding invalid early offset: {}:{}, topic: {}, partition: {}, from offset: {}, to: {}", consumer.host(), consumer.port(), topic, partition, offset, earliestOffset);
                        offset = earliestOffset;
                    // offset exceptions should only be thrown when offset < earliest, so this case shouldnt ever happen
                    } else {
                        throw e;
                    }
                // InvalidMessageSize is what you'll see when an invalid offset doesnt point at a batch, typically due to a human error that deletes the kafka logs on a broker.
                // When this happens, there's no reasonable way to slightly shift the offset to a valid state, so instead we restart consuming from the latest.  While it may
                // sometimes be safe (and correct) to restart consuming from the earliest, double-processing data is generally considered worse than skipping small amounts of
                // data.  Additionally, using latest instead of earliest prevents the sort of catastophe where offsets are repeatedly invalidated and data is reprocessed from
                // the beginning many times over.
                } catch (InvalidMessageSizeException e) {
                    long latestOffset = latestOffsetAvailable(consumer, topic, partition);
                    log.error("Error: ", e);
                    log.error("broker {}:{}, topic: {}, partition: {}, from offset: {} threw an invalid message size exception; the current offset does not point at " +
                              "a valid batch, so either the log data was wiped (resetting offsets) or corrupted - skipping ahead to offset {} to recover consuming from latest",
                            consumer.host(), consumer.port(), topic, partition, offset, latestOffset);
                    offset = latestOffset;
                }
                catch (Exception e) {
                    log.error("failed to consume from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partition, offset);
                    throw e;
                }
                if (messageSet != null) {
                    for (MessageAndOffset messageAndOffset : messageSet) {
                        putWhileRunning(messageQueue, new MessageWrapper(messageAndOffset, consumer.host(), topic, partition, sourceIdentifier), running);
                        offset = messageAndOffset.offset();
                    }
                }
            }
            log.info("finished consuming from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partition, offset);
        } catch (BenignKafkaException ignored) {
        } catch (KeeperException.NoNodeException e) {
            log.debug("previous broker {} is not currently up", brokerInfo.getBrokerId());
        } catch (Exception e) {
            log.error("kafka consume thread failed: ", e);
        } finally {
            latch.countDown();
        }
    }

    private static URI getBrokerURI(CuratorFramework zkClient, int brokerId) throws Exception {
        String brokerDataJson = StringSerializer.deserialize(zkClient.getData().forPath(ZkUtils.BrokerIdsPath() + "/" + brokerId));
        if (brokerDataJson != null) {
            String[] pieces = brokerDataJson.split(":");
            return new URI("http://" + pieces[1] + ":" + pieces[2]);
        }
        return null;
    }

    @Override
    public int compareTo(Object o) {
        return Integer.compare(this.partition, ((FetchTask) o).partition);
    }
}
