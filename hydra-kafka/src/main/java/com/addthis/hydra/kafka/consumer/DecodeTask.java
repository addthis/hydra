package com.addthis.hydra.kafka.consumer;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.data.io.kafka.KafkaSource.putWhileRunning;
import static com.addthis.hydra.data.io.kafka.MessageWrapper.messageQueueEndMarker;
import kafka.message.Message;

class DecodeTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DecodeTask.class);

    private final CountDownLatch decodeLatch;
    private final ListBundleFormat format;
    private final AtomicBoolean running;
    private final BlockingQueue<MessageWrapper> messageQueue;
    private final BlockingQueue<BundleWrapper> bundleQueue;

    public DecodeTask(CountDownLatch decodeLatch, ListBundleFormat format, AtomicBoolean running,
            BlockingQueue<MessageWrapper> messageQueue, BlockingQueue<BundleWrapper> bundleQueue) {
        this.running = running;
        this.messageQueue = messageQueue;
        this.bundleQueue = bundleQueue;
        this.decodeLatch = decodeLatch;
        this.format = format;
    }

    @Override
    public void run() {
        try {
            //noinspection StatementWithEmptyBody
            while (running.get() && decodeUnlessEnded()) ;
            log.info("finished decoding");
        } catch (BenignKafkaException ignored) {
        } catch (Exception e) {
            log.error("kafka decode thread failed: ", e);
            throw e;
        } finally {
            decodeLatch.countDown();
        }
    }

    @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
    private boolean decodeUnlessEnded() {
        MessageWrapper messageWrapper = null;
        try {
            messageWrapper = messageQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignored
        }
        if (messageWrapper == messageQueueEndMarker) {
            messageQueue.add(messageQueueEndMarker);
            return false;
        }
        if (messageWrapper != null) {
            Bundle bundle = null;
            try {
                Message message = messageWrapper.messageAndOffset.message();
                byte[] messageBytes = Arrays.copyOfRange(message.payload().array(), message.payload().arrayOffset(), message.payload().arrayOffset() + message.payload().limit());
                bundle = DataChannelCodec.decodeBundle(new ListBundle(format), messageBytes);
            } catch (Exception e) {
                log.error("failed to decode bundle from host: {}, topic: {}, partition: {}, offset: {}, bytes: {}",
                        messageWrapper.host, messageWrapper.topic, messageWrapper.partition, messageWrapper.messageAndOffset.offset(), messageWrapper.messageAndOffset.message().payloadSize());
                log.error("decode exception: ", e);
            }
            if (bundle != null) {
                putWhileRunning(bundleQueue, new BundleWrapper(bundle, messageWrapper.sourceIdentifier, messageWrapper.messageAndOffset.offset()), running);
            }
        }
        return true;
    }

}
