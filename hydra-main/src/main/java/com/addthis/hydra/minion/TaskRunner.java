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
package com.addthis.hydra.minion;

import java.io.IOException;

import com.addthis.basis.util.Backoff;

import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.job.mq.CommandTaskKick;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskRunner extends Thread {
    private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

    private boolean done;

    private final Minion minion;
    private final Backoff backoff = new Backoff(1000, 5000);

    public TaskRunner(Minion minion) {
        this.minion = minion;
    }

    public void stopTaskRunner() {
        done = true;
        interrupt();
    }

    @Override public void run() {
        while (!done) {
            QueueingConsumer.Delivery delivery = null;
            try {
                delivery = minion.batchJobConsumer.nextDelivery();
                CommandTaskKick kick = null;
                try {
                    kick = Jackson.defaultMapper().readValue(delivery.getBody(), CommandTaskKick.class);
                } catch(IOException e) {
                    log.warn("[task.runner] unknown command type : {}", e);
                    continue;
                }
                minion.insertJobKickMessage(kick);
                minion.kickNextJob();
                minion.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (InterruptedException ex) {
                log.warn("Interrupted while processing task messages");
                try {
                    minion.close();
                } catch (Exception e) {
                    log.error("Minion close throws an exception", e);
                } finally {
                    minion.shutdown();
                }
            } catch (ShutdownSignalException shutdownException) {
                log.warn("Received unexpected shutdown exception from rabbitMQ", shutdownException);
                try {
                    backoff.sleep();
                } catch (InterruptedException ignored) {
                    // interrupted while sleeping
                }
            } catch (Throwable ex) {
                try {
                    if (delivery != null) {
                        minion.channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    }
                } catch (Exception e) {
                    log.warn("[task.runner] unable to nack message delivery", ex);
                }
                log.warn("[task.runner] error: " + ex);
                if (!(ex instanceof ExecException)) {
                    log.error("Error nacking message", ex);
                }
                try {
                    minion.close();
                } catch (Exception e) {
                    log.error("Minion close throws an exception", e);
                } finally {
                    minion.shutdown();
                }
            }
        }
    }
}
