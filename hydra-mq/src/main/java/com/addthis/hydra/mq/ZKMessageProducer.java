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

import java.io.IOException;

import com.addthis.codec.jackson.Jackson;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMessageProducer<T> implements MessageProducer<T> {

    private static final Logger log = LoggerFactory.getLogger(ZKMessageProducer.class);

    private final CuratorFramework zkClient;

    public ZKMessageProducer(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void close() throws IOException {
        // Whomever passed us the client needs to shut it down.
    }

    @Override
    public void sendMessage(T message, String routingKey) throws IOException {
        try {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(routingKey, Jackson.defaultMapper().writeValueAsBytes(message));
            } catch (KeeperException.NodeExistsException e) {
                zkClient.setData().forPath(routingKey, Jackson.defaultMapper().writeValueAsBytes(message));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
