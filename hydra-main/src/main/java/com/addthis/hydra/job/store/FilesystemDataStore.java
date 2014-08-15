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
package com.addthis.hydra.job.store;

import java.io.File;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class FilesystemDataStore implements SpawnDataStore, Codable {
    private static final Logger log = LoggerFactory.getLogger(FilesystemDataStore.class);
    private static final boolean debug = Parameter.boolValue("spawn.datastore.fs.debug",false);

    private AtomicBoolean changed = new AtomicBoolean(false);
    private Thread thread;
    private File file;
    private long lastMod;

    @FieldConfig(codable = true, required = true)
    private TreeMap<String,String> map = new TreeMap<>();

    public FilesystemDataStore(final File file) throws Exception {
        this.file = file;
        Files.initDirectory(file.getParentFile());
        read();
        thread = new Thread("FilesystemDataStore Sync") {
            public void run() {
                while (true) {
                    try {
                        sleep(1000);
                        read();
                        write();
                    } catch (Exception ex) {
                        log.info("{} exiting", this.getName());
                        return;
                    }
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public String getDescription() {
        return "filesystem";
    }

    @Override
    public synchronized String get(String path) {
        if (debug) log.info("get :: ",path);
        return map.get(path);
    }

    @Override
    public synchronized Map<String, String> get(String[] paths) {
        if (debug) log.info("get :: ",paths);
        HashMap<String,String> map = new HashMap<>();
        for (String path : paths) {
            map.put(path, get(path));
        }
        return map;
    }

    @Override
    public synchronized void put(String path, String value) throws Exception {
        if (debug) log.info("put :: {} = {}", path, value);
        String last = map.put(path, value);
        if (last == null || value == null && last != value) {
            changed.set(true);
        } else if (last != null && value != null && !last.equals(value)) {
            changed.set(true);
        }
    }

    private String join(String parent, String child) {
        if (parent.endsWith("/")) {
            return parent + child;
        } else {
            return parent + "/" + child;
        }
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        put(join(parent,childId), value);
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        return get(join(parent,childId));
    }

    @Override
    public void deleteChild(String parent, String childId) {
        delete(join(parent,childId));
    }

    @Override
    public synchronized void delete(String path) {
        if (debug) log.info("delete :: {}", path);
        boolean removed = false;
        removed = (map.remove(path) != null);
        LinkedList<String> remove = new LinkedList<>();
        final String pre = path + "/";
        for (Map.Entry<String, String> e : map.tailMap(pre, true).entrySet()) {
            String key = e.getKey();
            if (!key.startsWith(pre)) {
                break;
            }
            remove.add(key);
        }
        for (String key : remove) {
            if (debug) log.info("delete.remove :: {}", key);
            removed |= (map.remove(key) != null);
        }
        if (removed) {
            changed.set(true);
        }
    }

    @Override
    public List<String> getChildrenNames(String path) {
        final LinkedList<String> list = new LinkedList<>();
        final String pre = path.endsWith("/") ? path : path + "/";
        for (Map.Entry<String, String> e : map.tailMap(pre, true).entrySet()) {
            String key = e.getKey();
            if (key.length() <= pre.length()) {
                break;
            }
            String child = key.substring(pre.length());
            if (!child.contains("/")) {
                list.add(child);
            }
        }
        return list;
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        HashMap<String,String> map = new HashMap<>();
        for (String child : getChildrenNames(path)) {
            map.put(child, get(join(path,child)));
        }
        return map;
    }

    private synchronized void read() throws Exception {
        if (file.exists() && file.isFile() && file.lastModified() > lastMod) {
            log.info("(re)loading");
            CodecJSON.decodeString(this, Bytes.toString(Files.read(file)));
            lastMod = file.lastModified();
        }
    }

    private synchronized void write() {
        if (changed.compareAndSet(true,false)) {
            try {
                if (debug) log.info("writing");
                Files.write(file, Bytes.toBytes(CodecJSON.encodeString(this)), false);
                lastMod = file.lastModified();
            } catch (Exception ex) {
                log.error("unable to write to datastore", ex);
            }
        }
    }

    @Override
    public void close() {
        thread.interrupt();
        write();
    }
}
