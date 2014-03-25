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

public class MeshMessageProducer implements MessageProducer {

    private static Logger log = LoggerFactory.getLogger(MeshMessageProducer.class);
    private static final long noticeRefreshTime = Parameter.longValue("mesh.queue.notice.interval", 1000);
    private static final boolean debug = Parameter.boolValue("mesh.queue.debug", false);
    private static final boolean removeFailed = Parameter.boolValue("mesh.queue.fail.remove", false);

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
        final FileReference wantRef;
        final String havePath;
        long lastNotify;

        public Queue(final FileReference ref, final String path) {
            super();
            this.wantRef = ref;
            this.havePath = path;
        }

        public boolean enqueue(Serializable message) {
            synchronized (this) {
                addLast(message);
            }
            long now = System.currentTimeMillis();
            if (now - lastNotify > noticeRefreshTime) {
                try {
                    Bytes.readFully(mesh.readFile(wantRef));
                    lastNotify = now;
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;
        }

        public Serializable dequeue() {
            return removeFirst();
        }
    }

    class HaveTargets {
        final String wantPath;
        final String havePath;
        final String routing;

        long lastUpdate;
        int lastPeerCount;
        LinkedList<Queue> queues = new LinkedList<>();
        LinkedList<FileReference> refs = new LinkedList<>();
        HashSet<String> registered = new HashSet<>();

        HaveTargets(final String wantPath, final String havePath, final String routing) {
            this.wantPath = wantPath;
            this.havePath = havePath;
            this.routing = routing;
        }

        void ensureHavePath(final FileReference wantRef, final String uuid) {
            final String fullPath = havePath + "/" + uuid;
            synchronized (registered) {
                if (registered.contains(fullPath)) {
                    return;
                }
                registered.add(fullPath);
            }
            if (debug) log.info("creating endpoint for "+fullPath);
            final Queue haveQueue = new Queue(wantRef,fullPath);
            synchronized (queues) {
                queues.add(haveQueue);
            }
            provider.setListener(fullPath, new com.addthis.meshy.service.message.MessageListener() {
                @Override
                public void requestContents(String fileName, Map<String, String> options, OutputStream out) throws IOException {
                    if (debug) log.info("topic producer request fileName={} queue={} options={}", fileName, haveQueue.size(), options);
                    if (options == null || options.size() == 0) {
                        Bytes.writeString("{items:" + haveQueue.size()+"}", out);
                    } else {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        synchronized (haveQueue) {
                            int fetch = Math.max(0, Math.min(haveQueue.size(), Integer.parseInt(options.get("fetch"))));
                            if (debug && fetch > 0) log.info("encoding {} objects", fetch);
                            if (fetch > 0) {
                                Bytes.writeInt(fetch, bos);
                                ObjectOutputStream oos = new ObjectOutputStream(bos);
                                while (haveQueue.size() > 0 && fetch-- > 0) {
                                    oos.writeObject(haveQueue.removeFirst());
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

        void send(Serializable message) {
            HashSet<Queue> queueFails = new HashSet<>();
            synchronized (queues) {
                for (Queue queue : queues) {
                    if (debug) log.info("queue have={} size={} msg={}", queue.havePath, queue.size(), message);
                    if (!queue.enqueue(message)) {
                        queueFails.add(queue);
                    }
                }
                /**
                 * TODO do we want to do this or retain the queue for when the node returns?
                 * potential mem leak if not spilled to disk
                 */
                if (removeFailed && queueFails.size() > 0){
                    if (debug) log.info("queue fails={} have={} routing={} queues={}", queueFails.size(), wantPath, routing, queues.size());
                    for (Queue queue : queueFails) {
                        boolean queueRemove = queues.remove(queue);
                        boolean regRemove = false;
                        synchronized (registered) {
                            regRemove = registered.remove(queue.havePath);
                        }
                        if (debug) log.info("queue remove={} have={} routing={} rm.queue={} rm.reg={}", queue.wantRef.name, wantPath, routing, queueRemove, regRemove);
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
                if (debug) log.info("update want={} routing={}", wantPath, routing);
                for (FileReference ref : mesh.listFiles(new String[] { wantPath })) {
                    InputStream in = mesh.readFile(ref);
                    String uuid = Bytes.readString(in);
                    int keyCount = Bytes.readInt(in);
                    while (keyCount-- > 0) {
                        String routingKey = Bytes.readString(in);
                        if (debug) log.info("update want={} peer={} routing={} key={}", wantPath, uuid, routing, routingKey);
                        if (routingKey.equals(routing)) {
                            ensureHavePath(ref, uuid);
                        }
                    }
                }
                lastUpdate = System.currentTimeMillis();
                lastPeerCount = peers;
            } catch (IOException e) {
                e.printStackTrace();
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
                targets = new HaveTargets(wantPath, havePath, routing);
                consumers.put(wantPath, targets);
            }
        }
        targets.update();
        targets.send(message);
    }
}
