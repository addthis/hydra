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
package com.addthis.hydra.job.minion;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import com.addthis.basis.util.Backoff;

import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.mq.RabbitQueueingConsumer;

import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskRunner extends Thread {
    private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

    private boolean done;

    private final Minion minion;
    private final boolean meshQueue;
    private final Backoff backoff = new Backoff(1000, 5000);

    public TaskRunner(Minion minion, boolean meshQueue) {
        this.minion = minion;
        this.meshQueue = meshQueue;
    }

    public void stopTaskRunner() {
        done = true;
        interrupt();
    }

    @Override public void run() {
        while (!done) {
            if (meshQueue) {
                try {
                    HostMessage hostMessage = minion.queuedHostMessages.take();
                    if (hostMessage.getMessageType() != CoreMessage.TYPE.CMD_TASK_KICK) {
                        log.warn("[task.runner] unknown command type : " + hostMessage.getMessageType());
                        continue;
                    }
                    CommandTaskKick kick = (CommandTaskKick) hostMessage;
                    minion.insertJobKickMessage(kick);
                    minion.kickNextJob();
                } catch (InterruptedException e) {
                    // ignore
                } catch (Exception e) {
                    log.error("Error sending meshQueue message", e);
                }
            } else {
                RabbitQueueingConsumer.Delivery delivery = null;
                try {
                    delivery = minion.batchJobConsumer.nextDelivery();
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(delivery.getBody()));
                    HostMessage hostMessage = (HostMessage) ois.readObject();
                    if (hostMessage.getMessageType() != CoreMessage.TYPE.CMD_TASK_KICK) {
                        log.warn("[task.runner] unknown command type : {}", hostMessage.getMessageType());
                        continue;
                    }
                    CommandTaskKick kick = (CommandTaskKick) hostMessage;
                    minion.insertJobKickMessage(kick);
                    minion.kickNextJob();
                    minion.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (InterruptedException ex) {
                    log.warn("Interrupted while processing task messages");
                    minion.shutdown();
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
                    minion.shutdown();
                }
            }
        }
    }
}
