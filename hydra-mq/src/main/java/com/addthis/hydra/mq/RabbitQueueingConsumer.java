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
package com.addthis.hydra.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RabbitQueueingConsumer extends DefaultConsumer {

    private final BlockingQueue<Delivery> queue;

    public RabbitQueueingConsumer(Channel channel) {
        super(channel);
        queue = new LinkedBlockingQueue<>();
    }

    @Override public void handleDelivery(String consumerTag,
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body)
    {
        this.queue.add(new Delivery(envelope, properties, body));
    }

    /**
     * Main application-side API: wait for the next message delivery and return it.
     * @return the next message
     * @throws InterruptedException if an interrupt is received while waiting
     * @throws ShutdownSignalException if the connection is shut down while waiting
     */
    public Delivery nextDelivery()
            throws InterruptedException, ShutdownSignalException
    {
        return queue.take();
    }

    @Override public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        // because we are using an HA connection we do not want to handle
        // the shutdown message here
    }

}
