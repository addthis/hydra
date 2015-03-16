package com.addthis.hydra.kafka.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.addthis.hydra.kafka.consumer.KafkaSource.putWhileRunning;

class MarkEndTask<T> implements Runnable {

    private final CountDownLatch decodeLatch;
    private final AtomicBoolean runningGuard;
    private final BlockingQueue<T> markableQueue;
    private final T marker;

    public MarkEndTask(CountDownLatch decodeLatch, AtomicBoolean runningGuard,
            BlockingQueue<T> markableQueue, T marker) {
        this.decodeLatch = decodeLatch;
        this.runningGuard = runningGuard;
        this.markableQueue = markableQueue;
        this.marker = marker;
    }

    @Override
    public void run() {
        awaitUninterruptably();
        putWhileRunning(markableQueue, marker, runningGuard);
    }

    void awaitUninterruptably() {
        while (true) {
            try {
                decodeLatch.await();
                return;
            } catch (InterruptedException ignored) {
            }
        }
    }

}
