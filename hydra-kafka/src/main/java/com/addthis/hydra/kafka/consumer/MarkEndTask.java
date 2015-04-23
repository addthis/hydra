/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
