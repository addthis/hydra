package com.addthis.hydra.kafka.consumer;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.task.source.SimpleMark;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.kafka.consumer.KafkaSource.putWhileRunning;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

class FetchTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FetchTask.class);

    private static final int fetchSize = Parameter.intValue(FetchTask.class + ".fetchSize", 1048576);
    private static final int timeout = Parameter.intValue(FetchTask.class + ".timeout", 10000);
    private static final int offsetAttempts = Parameter.intValue(FetchTask.class + ".offsetAttempts", 3);

    private KafkaSource kafkaSource;
    private final CountDownLatch fetchLatch;
    private final String topic;
    private final PartitionMetadata partition;
    private final DateTime startTime;

    public FetchTask(KafkaSource kafkaSource, CountDownLatch fetchLatch, String topic, PartitionMetadata partition, DateTime startTime) {
        this.kafkaSource = kafkaSource;
        this.fetchLatch = fetchLatch;
        this.topic = topic;
        this.partition = partition;
        this.startTime = startTime;
    }

    @Override
    public void run() {
        consume(kafkaSource.running, fetchLatch, topic, partition, kafkaSource.markDb, startTime, kafkaSource.messageQueue, kafkaSource.sourceOffsets);
    }

    private static void consume(AtomicBoolean running, CountDownLatch latch,
            String topic, PartitionMetadata partition, PageDB<SimpleMark> markDb, DateTime startTime,
            LinkedBlockingQueue<MessageWrapper> messageQueue, ConcurrentMap<String, Long> sourceOffsets) {
        SimpleConsumer consumer = null;
        try {
            if (!running.get()) {
                return;
            }
            // initialize consumer and offsets
            int partitionId = partition.partitionId();
            Broker broker = partition.leader();
            consumer = new SimpleConsumer(broker.host(), broker.port(), timeout, fetchSize, "kafka-source-consumer");
            String sourceIdentifier = topic + "-" + partitionId;
            final long endOffset = ConsumerUtils.latestOffsetAvailable(consumer, topic, partitionId, offsetAttempts);
            final SimpleMark previousMark = markDb.get(new DBKey(0, sourceIdentifier));
            long offset = -1;
            if (previousMark != null) {
                offset = previousMark.getIndex();
            } else if (startTime != null) {
                offset = ConsumerUtils.getOffsetBefore(consumer, topic, partitionId, startTime.getMillis(), offsetAttempts);
                log.info("no previous mark for host: {}, partition: {}, starting from offset: {}, closest to: {}", consumer.host(), partitionId, offset, startTime);
            }
            if (offset == -1) {
                log.info("no previous mark for host: {}:{}, topic: {}, partition: {}, no offsets available for startTime: {}, starting from earliest", consumer.host(), consumer.port(), topic, partitionId, startTime);
                offset = ConsumerUtils.earliestOffsetAvailable(consumer, topic, partitionId, offsetAttempts);
            } else if(offset > endOffset) {
                log.warn("initial offset for: {}:{}, topic: {}, partition: {} is beyond latest, {} > {}; kafka data was either wiped (resetting offsets) or corrupted - skipping " +
                         "ahead to offset {} to recover consuming from latest", consumer.host(), consumer.port(), topic, partitionId, offset, endOffset, endOffset);
                offset = endOffset;
                // Offsets are normally updated when the bundles are consumed from source.next() - since we wont be fetching any bundles to be consumed, we need
                // to update offset map (that gets persisted to marks) here.
                // The sourceOffsets map probably *should not* be modified anywhere else outside of next().
                sourceOffsets.put(sourceIdentifier, offset);
            }
            log.info("starting to consume topic: {}, partition: {} from broker: {}:{} at offset: {}, until offset: {}",
                    topic, partitionId, consumer.host(), consumer.port(), offset, endOffset);
            // fetch from broker, add to queue (decoder threads will process queue in parallel)
            while (running.get() && (offset < endOffset)) {
                FetchRequest request = new FetchRequestBuilder().addFetch(topic, partitionId, offset, fetchSize).build();
                FetchResponse response = consumer.fetch(request);
                short errorCode = response.errorCode(topic, partitionId);
                ByteBufferMessageSet messageSet = null;
                if (errorCode == ErrorMapping.NoError()) {
                    messageSet = response.messageSet(topic, partitionId);
                }
                // clamp out-of-range offsets
                else if(errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                    long earliestOffset = ConsumerUtils.earliestOffsetAvailable(consumer, topic, partitionId, offsetAttempts);
                    if (offset < earliestOffset) {
                        log.error("forwarding invalid early offset: {}:{}, topic: {}, partition: {}, from offset: {}, to: {}", consumer.host(), consumer.port(), topic, partition, offset, earliestOffset);
                        offset = earliestOffset;
                        // offset exceptions should only be thrown when offset < earliest, so this case shouldnt ever happen
                    } else {
                        long latestOffset = ConsumerUtils.latestOffsetAvailable(consumer, topic, partitionId, offsetAttempts);
                        log.error("rewinding invalid future offset: {}:{}, topic: {}, partition: {}, from offset: {}, to: {}", consumer.host(), consumer.port(), topic, partition, offset, latestOffset);
                        offset = latestOffset;
                    }
                }
                // partition was moved/rebalanced in background, so this consumer's host no longer has data
                else if(errorCode == ErrorMapping.NotLeaderForPartitionCode() || errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
                    Broker newLeader = ConsumerUtils.getNewLeader(consumer, topic, partitionId);
                    log.warn("current partition was moved off of current host while consuming, reconnecting to new leader; topic: {}-{}, old: {}:{}, new leader: {}:{}",
                            topic, partitionId, consumer.host(), consumer.port(), newLeader.host(), newLeader.port());
                    consumer.close();
                    consumer = new SimpleConsumer(newLeader.host(), newLeader.port(), timeout, fetchSize, "kafka-source-consumer");
                }
                // any other error
                else if(errorCode != ErrorMapping.NoError()) {
                    log.error("failed to consume from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partitionId, offset);
                    throw new RuntimeException(ErrorMapping.exceptionFor(errorCode));
                }

                if (messageSet != null) {
                    for (MessageAndOffset messageAndOffset : messageSet) {
                        putWhileRunning(messageQueue, new MessageWrapper(messageAndOffset, consumer.host(), topic, partitionId, sourceIdentifier), running);
                        offset = messageAndOffset.nextOffset();
                    }
                }
            }
            log.info("finished consuming from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partitionId, offset);
        } catch (BenignKafkaException ignored) {
        } catch (Exception e) {
            log.error("kafka consume thread failed: ", e);
        } finally {
            latch.countDown();
            if(consumer != null) {
                consumer.close();
            }
        }
    }
}
