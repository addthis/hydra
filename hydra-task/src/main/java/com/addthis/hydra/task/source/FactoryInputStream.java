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
package com.addthis.hydra.task.source;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.Socket;

import java.util.IdentityHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;


/**
 * This section of the job specification handles input sources.
 * <p>The following factories are available:
 * <ul>
 * <li>{@link FileInputStreamSource file}</li>
 * <li>{@link InjectorStreamSource inject}</li>
 * <li>{@link SocketInputStreamSource socket}</li>
 * </ul>
 *
 * @user-reference
 */
@Pluggable("factory input stream")
public abstract class FactoryInputStream implements Codable {

    /**
     * @return an InputStream
     */
    public abstract InputStream createInputStream() throws IOException;

    /**
     * @user-reference
     */
    public static final class FileInputStreamSource extends FactoryInputStream {

        @FieldConfig(codable = true, required = true)
        private String file;

        @Override
        public InputStream createInputStream() throws IOException {
            return new FileInputStream(file);
        }
    }

    /**
     * @user-reference
     */
    public static final class SocketInputStreamSource extends FactoryInputStream {

        @FieldConfig(codable = true, required = true)
        private String host;
        @FieldConfig(codable = true, required = true)
        private int port;

        @Override
        public InputStream createInputStream() throws IOException {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(60000 * 5);
            return socket.getInputStream();
        }
    }

    /**
     * @user-reference
     */
    public static final class InjectorStreamSource extends FactoryInputStream {

        private static final Object lock = new Object();
        public static final String DefautlInjectorKey = "secretDefaultInjectorKey";
        private static final IdentityHashMap<String, LinkedBlockingQueue<InputStream>> park = new IdentityHashMap<>();

        public static final void inject(String key, InputStream in) {
            key = key.intern();
            synchronized (lock) {
                try {
                    LinkedBlockingQueue<InputStream> queue = null;
                    synchronized (park) {
                        queue = park.get(key);
                        if (queue == null) {
                            queue = new LinkedBlockingQueue<>();
                            park.put(key, queue);
                            key.notifyAll();
                        }
                    }
                    queue.put(in);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * The default value is "secretDefaultInjectorKey".
         */
        @FieldConfig(codable = true)
        private String key = DefautlInjectorKey;

        private LinkedBlockingQueue<InputStream> queue = null;

        @Override
        public InputStream createInputStream() throws IOException {
            try {
                while (queue == null) {
                    key = key.intern();
                    synchronized (lock) {
                        synchronized (park) {
                            queue = park.get(key);
                        }
                        if (queue == null) {
                            key.wait();
                            continue;
                        }
                        break;
                    }
                }
                return queue.take();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
