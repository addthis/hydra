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
import java.io.Serializable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMessageProducer implements MessageProducer {

    private static Logger log = LoggerFactory.getLogger(ZKMessageProducer.class);

    private CuratorFramework zkClient;
    private ObjectMapper mapper;

    public ZKMessageProducer(CuratorFramework zkClient) {
        this.zkClient = zkClient;
        mapper = new ObjectMapper();
        try {
            open();
        } catch (IOException e) {
            log.warn("", "[zk.producer] error opening client: " + e);
        }
    }

    @Override
    public void open() throws IOException {
        // Working client is the only setup required.
    }

    @Override
    public void close() throws IOException {
        // Whomever passed us the client needs to shut it down.
    }

    @Override
    public void sendMessage(Serializable message, String routingKey) throws IOException {
        try {
            try {
                zkClient.create().creatingParentsIfNeeded().forPath(routingKey, mapper.writeValueAsBytes(message));
            } catch (KeeperException.NodeExistsException e) {
                zkClient.setData().forPath(routingKey, mapper.writeValueAsBytes(message));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
