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
import com.addthis.basis.util.Strings;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.message.MessageFileProvider;
import org.jboss.netty.channel.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MeshMessageConsumer implements MessageConsumer {

    private static Logger log = LoggerFactory.getLogger(MeshMessageConsumer.class);
    private final static long scanInterval = Parameter.longValue("mesh.queue.scan.interval", 30000);
    private final static long pollInterval = Parameter.longValue("mesh.queue.poll.interval", 10000);
    private static final boolean debug = Parameter.boolValue("mesh.queue.debug", false);

    private final MeshyClient mesh;
    private final String uuid;
    private final String topic;
    private final HashSet<MessageListener> listeners = new HashSet<MessageListener>();
    private final HashSet<String> routingKeys = new HashSet<>();
    private final HashSet<String> targets = new HashSet<String>();
    private final HashSet<String> sources = new HashSet<String>();
    private final IntervalTimer scanner;
    private final IntervalTimer poller;

    private volatile LinkedList<FileReference> fileSources = new LinkedList<>();
    private MessageFileProvider provider;
    private String findPaths[];

   public MeshMessageConsumer(final MeshyClient mesh, final String topic, final String uuid) {
        this.mesh = mesh;
        this.uuid = uuid;
        this.topic = topic;
        try {
            open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        poller = new IntervalTimer("mesh queue source poller", pollInterval) {
            public void task() throws Exception {
                pollAll();
            }
        };
        scanner = new IntervalTimer("mesh queue source scanner", scanInterval) {
            public void task() throws Exception {
                scan();
            }
        };
        addRoutingKey(uuid);
    }

    public MeshMessageConsumer addRoutingKey(String key) {
        routingKeys.add(key);
        return this;
    }

    private final AtomicInteger scans = new AtomicInteger(0);

    private void scan() {
        if (!scans.compareAndSet(0, 1)) {
            log.info("scan kicked while scan still running");
            return;
        }
        final LinkedList<FileReference> newSources = new LinkedList<>();
        try {
            if (debug) log.info("scanning : "+ Strings.join(findPaths, ", "));
            mesh.listFiles(findPaths, new MeshyClient.ListCallback() {
                @Override
                public void receiveReference(FileReference ref) {
                    if (debug) log.info("new source : "+ ref);
                    newSources.add(ref);
                }

                @Override
                public void receiveReferenceComplete() {
                    fileSources = newSources;
                    if (debug) log.info("new sources : "+ fileSources);
                    scans.set(0);
                    poller.bump();
                }
            });
        } catch (IOException ex) {
            log.warn("", ex);
        }
    }

    private void pollAll() {
        for (FileReference fileSource : fileSources) {
            poll(fileSource);
        }
    }

    private void poll(final FileReference fileRef) {
        HashMap<String,String> options = new HashMap<String,String>();
        options.put("fetch","100");
        try {
            if (debug) log.info("polling ref={} options={}", fileRef, options);
            InputStream in = mesh.readFile(fileRef,options);
            int fetch = Bytes.readInt(in);
            if (debug) log.info("recv {} items ref={}", fetch, fileRef.name);
            ObjectInputStream ois = new ObjectInputStream(in);
            while (fetch-- > 0) {
                Serializable message = (Serializable) ois.readObject();
                if (message == null) break;
                if (debug) log.info("recv ref={} msg={} targets={}", fileRef.name, message, listeners.size());
                for (MessageListener listener : listeners) {
                    try {
                        listener.onMessage(message);
                    } catch (Exception ex) {
                        log.warn("listener error", ex);
                    }
                }
            }
        } catch (EOFException e) {
            // expected
        } catch (ChannelException e) {
            // on shutdown
            log.info("shutting down? message={}", e.getMessage());
        } catch (Exception ex) {
            log.warn("poll error", ex);
        }
    }

    @Override
    public void open() throws IOException {
        provider = new MessageFileProvider(mesh);
        final String wantPath = "/queue/want/" + topic + "/" + uuid;
        sources.add("/queue/have/" + topic + "/" + uuid);
        targets.add(wantPath);
        provider.setListener(wantPath, new com.addthis.meshy.service.message.MessageListener() {
            @Override
            public void requestContents(String fileName, Map<String, String> options, OutputStream out) throws IOException {
                if (debug) log.info("topic consumer request fileName={}, options={} on topic={}", fileName, options, topic);
                Bytes.writeString(uuid, out);
                Bytes.writeInt(routingKeys.size(), out);
                for (String routingKey : routingKeys) {
                    Bytes.writeString(routingKey, out);
                }
                out.close();
                poller.bump();
            }
        });
        if (debug) log.info("listening to " + sources);
        findPaths = sources.toArray(new String[sources.size()]);
    }

    @Override
    public void close() throws IOException {
        for (String path : targets) {
            provider.deleteListener(path);
        }
        targets.clear();
        sources.clear();
    }

    @Override
    public boolean addMessageListener(final MessageListener messageListener) {
        return listeners.add(messageListener);
    }

    @Override
    public boolean removeMessageListener(final MessageListener messageListener) {
        return listeners.remove(messageListener);
    }

    /* timed task that can be "bumped" to run now */
    abstract class IntervalTimer extends Thread {

        IntervalTimer(final String name, final long timeout) {
            super(name);
            this.timeout = timeout;
            setDaemon(true);
            start();
        }

        private final long timeout;
        private final Object bumper = new Object();

        public abstract void task() throws Exception ;

        public void bump() {
            synchronized (bumper) {
                bumper.notify();
            }
        }

        public void run() {
            if (debug) log.info("started with timeout "+timeout);
            try {
                loop();
            } finally {
                log.info("exited");
            }
        }

        public void loop() {
            while (true) {
                try {
                    if (debug) log.info("task start");
                    task();
                    if (debug) log.info("task end");
                } catch (Exception ex) {
                    log.warn("", ex);
                }
                try {
                    synchronized (bumper) {
                        bumper.wait(timeout);
                    }
                } catch (Exception ex) {
                    log.warn("", ex);
                }
            }
        }
    }
}
