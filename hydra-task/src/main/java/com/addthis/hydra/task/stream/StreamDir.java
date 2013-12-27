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
package com.addthis.hydra.task.stream;

import java.io.File;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.ReadMuxFile;
import com.addthis.muxy.ReadMuxFileDirectoryCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO add transparent MUX'd file stream support
 */
public final class StreamDir {

    private static final Logger logger = LoggerFactory.getLogger(StreamDir.class.getSimpleName());

    /**
     * LRU cache of StreamDir's
     */
    private static final LinkedHashMap<String, StreamDir> dirCache = new LinkedHashMap<String, StreamDir>() {
        protected boolean removeEldestEntry(Map.Entry<String, StreamDir> eldest) {
            if (dirCache.size() > maxDirs || (cachedFiles > maxFiles && dirCache.size() > 1)) {
                StreamDir dir = eldest.getValue();
                synchronized (dirCache) {
                    cachedFiles -= (dir.files.length + dir.dirs.length);
                }
                return true;
            }
            return false;
        }
    };

    /* max time to cache dir contents */
    private static long maxDirCacheAge = Long.parseLong(System.getProperty("streamdir.cache.dir.maxcacheage", "360000"));
    /* max # of dirs to cache */
    private static int maxDirs = Integer.parseInt(System.getProperty("streamdir.cache.dir", "10000"));
    /* max # of files to cache across all cached dirs */
    private static int maxFiles = Integer.parseInt(System.getProperty("streamdir.cache.file", "100000"));
    /* count of cached files across cached dirs */
    private static int cachedFiles = 0;

    public static void setMaxDirCache(int max) {
        maxDirs = max;
    }

    public static void setMaxFileCache(int max) {
        maxFiles = max;
    }

    /**
     * global entry point for finding a StreamDir and maintaining LRU cache
     */
    public static StreamDir getStreamDir(File dir) {
        synchronized (dirCache) {
            StreamDir cached = dirCache.get(dir.getPath());
            long lastMod = dir.lastModified();
            if (cached == null || cached.lastmod < lastMod || System.currentTimeMillis() - cached.updated > maxDirCacheAge) {
                List<File> listDir = null;
                List<StreamFile> listFile = null;
                if (MuxFileDirectory.isMuxDir(dir.toPath())) {
                    try {
                        Collection<ReadMuxFile> files = ReadMuxFileDirectoryCache.listFiles(dir);
                        listFile = new ArrayList<>(files.size());
                        listDir = new ArrayList<>(0);
                        for (ReadMuxFile meta : files) {
                            listFile.add(new StreamFileNative(dir.getPath().concat("/").concat(meta.getName()), meta.getLength(), meta.getLastModified(), null));
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    File files[] = dir.listFiles();
                    if (files != null) {
                        listFile = new ArrayList<StreamFile>(files.length);
                        listDir = new ArrayList<File>(files.length);
                        for (File file : files) {
                            if (file.isFile()) {
                                listFile.add(new StreamFileNative(file));
                            } else if (file.isDirectory()) {
                                listDir.add(file);
                            }
                        }
                    } else {
                        listFile = new ArrayList<StreamFile>(0);
                        listDir = new ArrayList<File>(0);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("refreshed dir cache for " + dir + " lastMod=" + lastMod + " files=" + listFile.size() + " dirs=" + listDir.size());
                }
                cached = new StreamDir(lastMod, listDir.toArray(new File[listDir.size()]), listFile.toArray(new StreamFile[listFile.size()]));
                dirCache.put(dir.getPath(), cached);
            }
            return cached;
        }
    }

    private final long updated;
    private final long lastmod;
    private final StreamFile files[];
    private final File dirs[];

    public StreamDir(long lastmod, File dirs[], StreamFile files[]) {
        this.updated = System.currentTimeMillis();
        this.lastmod = lastmod;
        this.dirs = dirs;
        this.files = files;
        synchronized (dirCache) {
            cachedFiles += (files.length + dirs.length);
        }
    }

    public File[] getDirs() {
        return dirs;
    }

    public StreamFile[] getFiles() {
        return files;
    }
}
