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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.addthis.bark.ZkHelpers;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMessageConsumer<T extends Serializable> implements MessageConsumer {

    private static Logger log = LoggerFactory.getLogger(ZKMessageProducer.class);


    private ZkClient zkClient;
    private ObjectMapper mapper;
    private String path;
    private Class<T> clazz;
    private TypeReference typeReference;
    private final Set<MessageListener> messageListeners = new HashSet<MessageListener>();
    private final IZkDataListener dataListener = new ZkDataListener();

    public ZkMessageConsumer(ZkClient zkClient, String path, MessageListener messageListener, final TypeReference<T> typeReference) {
        this.typeReference = typeReference;
        init(zkClient, path, messageListener);
    }

    public ZkMessageConsumer(ZkClient zkClient, String path, MessageListener messageListener, final Class<T> clazz) {
        this.clazz = clazz;
        init(zkClient, path, messageListener);
    }

    private void init(ZkClient zkClient, String path, MessageListener messageListener) {
        this.zkClient = zkClient;
        this.path = path;
        mapper = new ObjectMapper();
        addMessageListener(messageListener);
        try {
            open();
            ZkHelpers.makeSurePersistentPathExists(zkClient, path);
            notifyListeners(zkClient.getChildren(path));
            zkClient.subscribeChildChanges(path, new IZkChildListener() {
                @Override
                public void handleChildChange(String path, List<String> values) throws Exception {
                    notifyListeners(values);
                }
            });
        } catch (IOException e) {
            log.warn("[zk.producer] error opening client: " + e);
        }
    }


    private void notifyListeners(List<String> values) throws IOException {
        if (values != null && values.size() > 0) {
            for (String node : values) {
                zkClient.subscribeDataChanges(path + "/" + node, dataListener);
                String json = ZkHelpers.readData(zkClient, path + "/" + node);
                notifyListeners(json);
            }

        }
    }

    private void notifyListeners(String json) throws IOException {
        // A child znode may be used for another purpose and have no
        // data in it.
        if (json == null) {
            log.warn("[warning] got null notification.  Ignoring");
            return;
        }
        T message;
        if (clazz != null) {

            message = mapper.readValue(json, clazz);
        } else {
            message = mapper.<T>readValue(json, typeReference);
        }

        for (MessageListener listener : messageListeners) {
            listener.onMessage(message);
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

    public boolean addMessageListener(MessageListener hostMessageListener) {
        return messageListeners.add(hostMessageListener);
    }

    public boolean removeMessageListener(MessageListener hostMessageListener) {
        return messageListeners.remove(hostMessageListener);
    }

    private class ZkDataListener implements IZkDataListener {

        @Override
        public void handleDataChange(String path, Object json) throws Exception {
            notifyListeners((String) json);
        }

        @Override
        public void handleDataDeleted(String path) throws Exception {
            zkClient.unsubscribeDataChanges(path, dataListener);
        }
    }
}
