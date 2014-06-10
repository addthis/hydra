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

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.message.MessageFileProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MeshMessageProducer implements MessageProducer {

    private static Logger log = LoggerFactory.getLogger(MeshMessageProducer.class);
    private static final long noticeRefreshTime = Parameter.longValue("mesh.queue.notice.interval", 1000);
    private static final boolean debug = Parameter.boolValue("mesh.queue.debug", false);
    private static final boolean removeFailed = Parameter.boolValue("mesh.queue.fail.remove", true);

    private final MeshyClient mesh;
    private final String topic;
    private final HashMap<String,HaveTargets> consumers = new HashMap<String,HaveTargets>();

    private MessageFileProvider provider;

    public MeshMessageProducer(final MeshyClient mesh, final String topic) {
        this.mesh = mesh;
        this.topic = topic;
        try {
            open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void open() throws IOException {
        provider = new MessageFileProvider(mesh);
    }

    @Override
    public void close() throws IOException {
        synchronized (consumers) {
            for (String path : consumers.keySet()) {
                provider.deleteListener(path);
            }
            consumers.clear();
        }
    }

    class Queue extends LinkedList<Serializable> {
        final AtomicBoolean hasEndpoint = new AtomicBoolean(false);
        final HashSet<String> routing = new HashSet<>();
        final FileReference wantRef;
        final String havePath;
        long lastNotify;

        public Queue(final FileReference ref, final String path) {
            super();
            this.wantRef = ref;
            this.havePath = path;
        }

        public boolean hasRouting(final String key) {
            synchronized (routing) {
                return routing.contains(key);
            }
        }

        public void addRouting(final String key) {
            synchronized (routing) {
                routing.add(key);
            }
        }

        public boolean enqueue(final Serializable message) {
            ensureEndpoint();
            synchronized (this) {
                addLast(message);
            }
            long now = System.currentTimeMillis();
            if (now - lastNotify > noticeRefreshTime) {
                try {
                    Bytes.readFully(mesh.readFile(wantRef));
                    lastNotify = now;
                    return true;
                } catch (IOException ex) {
                    log.warn("", ex);
                    return false;
                }
            }
            return true;
        }

        void ensureEndpoint() {
            if (!hasEndpoint.compareAndSet(false, true)) {
                return;
            }
            if (debug) log.info("creating endpoint for "+havePath);
            provider.setListener(havePath, new com.addthis.meshy.service.message.MessageListener() {
                @Override
                public void requestContents(String fileName, Map<String, String> options, OutputStream out) throws IOException {
                    if (debug) log.info("topic producer request fileName={} queue={} options={}", fileName, size(), options);
                    if (options == null || options.size() == 0) {
                        out.write(Bytes.toBytes("{items:" + size()+",keys:\""+routing+"\"}"));
                    } else {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        synchronized (Queue.this) {
                            int fetch = Math.max(0, Math.min(size(), Integer.parseInt(options.get("fetch"))));
                            if (debug) log.info("encoding {} objects", fetch);
                            Bytes.writeInt(fetch, bos);
                            if (fetch > 0) {
                                ObjectOutputStream oos = new ObjectOutputStream(bos);
                                while (size() > 0 && fetch-- > 0) {
                                    oos.writeObject(removeFirst());
                                }
                                oos.close();
                            }
                        }
                        out.write(bos.toByteArray());
                    }
                    out.close();
                }
            });
        }

        void removeEndpoint() {
            if (hasEndpoint.compareAndSet(true, false)) {
                if (debug) log.info("deleting endpoint for "+havePath);
                provider.deleteListener(havePath);
            }
        }

        public Serializable dequeue() {
            return removeFirst();
        }
    }

    class HaveTargets {
        final String wantPath;
        final String havePath;

        long lastUpdate;
        int lastPeerCount;
        HashMap<String,Queue> queues = new HashMap<>();
        LinkedList<FileReference> refs = new LinkedList<>();

        HaveTargets(final String wantPath, final String havePath) {
            this.wantPath = wantPath;
            this.havePath = havePath;
        }

        void ensureHaveQueue(final FileReference wantRef, final String uuid, final String routing) {
            final String fullPath = havePath + "/" + uuid;
            Queue queue = null;
            synchronized (queues) {
                queue = queues.get(fullPath);
                if (queue != null) {
                    queue.addRouting(routing);
                    return;
                }
                queue = new Queue(wantRef, fullPath);
                queues.put(fullPath, queue);
            }
            final Queue haveQueue = queue;
            haveQueue.addRouting(routing);
        }

        void send(final Serializable message, final String routing) {
            HashSet<String> queueFails = new HashSet<>();
            synchronized (queues) {
                for (Map.Entry<String, Queue> entry : queues.entrySet()) {
                    String path = entry.getKey();
                    Queue queue = entry.getValue();
                    if (debug) log.info("queue have={} size={} msg={}", queue.havePath, queue.size(), message);
                    if (queue.hasRouting(routing) && !queue.enqueue(message)) {
                        queueFails.add(path);
                    }
                }
                /**
                 * TODO do we want to do this or retain the queue for when the node returns?
                 * potential mem leak if not spilled to disk
                 */
                if (removeFailed && queueFails.size() > 0){
                    if (debug) log.info("queue fails={} have={} routing={} queues={}", queueFails.size(), wantPath, routing, queues.size());
                    for (String path : queueFails) {
                        Queue queueRemove = queues.remove(path);
                        queueRemove.removeEndpoint();
                        if (debug) log.info("queue remove={} have={} routing={} rm.queue={}", path, wantPath, routing, queueRemove);
                    }
                }
            }
        }

        void update() {
            long now = System.currentTimeMillis();
            int peers = mesh.getPeeredCount();
            if (now - lastUpdate < 60000 && peers == lastPeerCount) {
                return;
            }
            try {
                if (debug) log.info("update want={}", wantPath);
                for (FileReference ref : mesh.listFiles(new String[] { wantPath })) {
                    HashMap<String,String> opt = new HashMap<>();
                    opt.put("scan","scan");
                    InputStream in = mesh.readFile(ref, opt);
                    String uuid = Bytes.readString(in);
                    int keyCount = Bytes.readInt(in);
                    while (keyCount-- > 0) {
                        String routingKey = Bytes.readString(in);
                        if (debug) log.info("update want={} peer={} key={}", wantPath, uuid, routingKey);
                        ensureHaveQueue(ref, uuid, routingKey);
                    }
                }
                lastUpdate = System.currentTimeMillis();
                lastPeerCount = peers;
            } catch (IOException ex) {
                log.warn("", ex);
            }
        }
    }

    @Override
    public void sendMessage(final Serializable message, final String routing) throws IOException {
        if (debug) log.info("submit topic={} routing={} message={}", topic, routing, message);
        final String wantPath = "/queue/want/"+topic+"/*";
        final String havePath = "/queue/have/"+topic;
        HaveTargets targets = null;
        synchronized (consumers) {
            targets = consumers.get(wantPath);
            if (targets == null) {
                targets = new HaveTargets(wantPath, havePath);
                consumers.put(wantPath, targets);
            }
        }
        targets.update();
        targets.send(message, routing);
    }
}
