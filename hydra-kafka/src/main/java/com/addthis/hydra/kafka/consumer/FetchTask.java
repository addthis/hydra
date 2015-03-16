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
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

class FetchTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FetchTask.class);

    private static final int fetchSize = Parameter.intValue("hydra.kafka.fetchSize", 1048576);
    private static final int timeout = Parameter.intValue("hydra.kafka.timeout", 10000);

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

    private static long earliestOffsetAvailable(SimpleConsumer consumer, String topic, int partition) {
        return ConsumerUtils.getOffsetsBefore(consumer, topic, partition, OffsetRequest.EarliestTime())[0];
    }

    private static long latestOffsetAvailable(SimpleConsumer consumer, String topic, int partition) {
        return ConsumerUtils.getOffsetsBefore(consumer, topic, partition, OffsetRequest.LatestTime())[0];
    }

    private static void consume(AtomicBoolean running, CountDownLatch latch,
            String topic, PartitionMetadata partition, PageDB<SimpleMark> markDb, DateTime startTime,
            LinkedBlockingQueue<MessageWrapper> messageQueue, ConcurrentMap<String, Long> sourceOffsets) {
        try {
            if (!running.get()) {
                return;
            }
            // initialize consumer and offsets
            int partitionId = partition.partitionId();
            Broker broker = partition.leader();
            final SimpleConsumer consumer = new SimpleConsumer(broker.host(), broker.port(), timeout, fetchSize, "kafka-source-consumer");
            String sourceIdentifier = topic + "-" + partitionId;
            final long endOffset = latestOffsetAvailable(consumer, topic, partitionId);
            final SimpleMark previousMark = markDb.get(new DBKey(0, sourceIdentifier));
            long offset = -1;
            if (previousMark != null) {
                offset = previousMark.getIndex();
            } else if (startTime != null) {
                long[] offsets = ConsumerUtils.getOffsetsBefore(consumer, topic, partitionId, startTime.getMillis());
                if (offsets.length == 1) {
                    log.info("no previous mark for host: {}, partition: {}, starting from offset: {}, closest to: {}", consumer.host(), partition, offsets[0], startTime);
                    offset = offsets[0];
                }
            }
            if (offset == -1) {
                log.info("no previous mark for host: {}:{}, topic: {}, partition: {}, no offsets available for startTime: {}, starting from earliest", consumer.host(), consumer.port(), topic, partition, startTime);
                offset = earliestOffsetAvailable(consumer, topic, partitionId);
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
                FetchRequest request = new FetchRequestBuilder().addFetch(topic, partitionId, offset, fetchSize).build();
                FetchResponse response = consumer.fetch(request);
                short errorCode = response.errorCode(topic, partitionId);
                ByteBufferMessageSet messageSet = null;
                if (errorCode == ErrorMapping.NoError()) {
                    messageSet = response.messageSet(topic, partitionId);
                }
                // clamp out-of-range offsets
                else if(errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                    long earliestOffset = earliestOffsetAvailable(consumer, topic, partitionId);
                    if (offset < earliestOffset) {
                        log.error("forwarding invalid early offset: {}:{}, topic: {}, partition: {}, from offset: {}, to: {}", consumer.host(), consumer.port(), topic, partition, offset, earliestOffset);
                        offset = earliestOffset;
                        // offset exceptions should only be thrown when offset < earliest, so this case shouldnt ever happen
                    } else {
                        long latestOffset = latestOffsetAvailable(consumer, topic, partitionId);
                        log.error("rewinding invalid future offset: {}:{}, topic: {}, partition: {}, from offset: {}, to: {}", consumer.host(), consumer.port(), topic, partition, offset, latestOffset);
                        offset = latestOffset;
                    }
                }
                // any other error
                else if(errorCode != ErrorMapping.NoError()) {
                    log.error("failed to consume from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partition, offset);
                    throw new RuntimeException(ErrorMapping.exceptionFor(errorCode));
                }

                if (messageSet != null) {
                    for (MessageAndOffset messageAndOffset : messageSet) {
                        putWhileRunning(messageQueue, new MessageWrapper(messageAndOffset, consumer.host(), topic, partitionId, sourceIdentifier), running);
                        offset = messageAndOffset.nextOffset();
                    }
                }
            }
            log.info("finished consuming from broker: {}:{}, topic: {}, partition: {}, offset: {}", consumer.host(), consumer.port(), topic, partition, offset);
        } catch (BenignKafkaException ignored) {
        } catch (Exception e) {
            log.error("kafka consume thread failed: ", e);
        } finally {
            latch.countDown();
        }
    }
}
