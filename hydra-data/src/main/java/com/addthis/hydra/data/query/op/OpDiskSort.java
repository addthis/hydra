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
package com.addthis.hydra.data.query.op;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.UUID;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.bundle.io.DataChannelWriter;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.muxy.MuxFile;
import com.addthis.muxy.MuxFileDirectory;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * <p>This query operation <span class="hydra-summary">performs a disk-backed sort</span>.
 * <p/>
 * <p>The syntax is dsort=[cols]:[type]:[direction]. [cols] is one or more columns
 * separated by commas. Type is a sequence of either "n" for numeric or "s" for string.
 * Direction is a sequence of either "a" for ascending or "d" for descending. The lengths
 * of [type] and [direction] must be equal to the number of column specified.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * 0 A 3
 * 1 A 1
 * 1 B 2
 * 0 A 5
 *
 * dsort=0,1,2:nsn:add
 *
 * 0 A 5
 * 0 A 3
 * 1 B 2
 * 1 A 1
 * </pre>
 *
 * @user-reference
 * @hydra-name dsort
 */
public class OpDiskSort extends AbstractRowOp {

    private static final Logger log = LoggerFactory.getLogger(OpDiskSort.class);
    private static final int CHUNK_ROWS = Parameter.intValue("op.disksort.chunk.rows", 5000);
    private static final int CHUNK_MERGES = Parameter.intValue("op.disksort.chunk.merges", 1000);
    private static final int GZTYPE = Parameter.intValue("op.disksort.gz.type", 0);

    private final Bundle buffer[] = new Bundle[CHUNK_ROWS + 1];
    private final BundleFactory factory = new ListBundle();
    private final QueryStatusObserver queryStatusObserver;

    private Path tempDir;
    private String[] cols;
    private char[] type;
    private char[] dir;
    private MuxFileDirectory mfm;
    private int bufferIndex = 0;
    private BundleComparator comparator;
    private BundleComparator comparatorSS;
    private int chunk = 0;

    public OpDiskSort(String args, String tempDirString, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        init(args, tempDirString);
    }

    private void init(String args, String tempDirString) {
        try {
            tempDir = Paths.get(tempDirString, String.valueOf(UUID.randomUUID()));
            Files.createDirectories(tempDir);
            mfm = new MuxFileDirectory(tempDir, null);
            mfm.setDeleteFreed(true);
            log.debug("tempDir={} mfm={}", tempDir, mfm);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        StringTokenizer st = new StringTokenizer(args, ":");
        cols = Strings.splitArray(st.hasMoreElements() ? st.nextToken() : "0", ",");

        String ts = st.hasMoreElements() ? st.nextToken() : "s";
        while (ts.length() < cols.length) {
            ts = ts.concat(ts.substring(0, 1));
        }
        type = ts.toCharArray();

        String ds = st.hasMoreElements() ? st.nextToken() : "a";
        while (ds.length() < cols.length) {
            ds = ds.concat(ds.substring(0, 1));
        }
        dir = ds.toCharArray();

        comparator = new BundleComparator();
        comparatorSS = new BundleComparator();
    }

    @Override
    public void close() throws IOException {
        cleanup();
        if (getNext() != null) {
            getNext().close();
        }
    }

    private void cleanup() {
        if (Files.exists(tempDir)) {
            boolean success = com.addthis.basis.util.Files.deleteDir(tempDir.toFile());
            if (!success) {
                log.warn("ERROR while deleting {} for disk sort", tempDir);
            }
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (bufferIndex > CHUNK_ROWS) {
            dumpBufferToMFM();
        }
        buffer[bufferIndex++] = row;
        return null;
    }

    private void dumpBufferToMFM() {
        if (bufferIndex > 0) {
            log.debug("dumpBufferToMFM buffer={} chunk={}", bufferIndex, chunk);
            try {
                Arrays.sort(buffer, 0, bufferIndex, comparator);
                MuxFile meta = mfm.openFile("l0-c" + (chunk++), true);
                OutputStream out = wrapOutputStream(meta.append());
                DataChannelWriter writer = new DataChannelWriter(out);
                for (int i = 0; i < bufferIndex; i++) {
                    writer.write(buffer[i]);
                }
                writer.close();
                out.close();
                meta.sync();
                bufferIndex = 0;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    // TODO: We really need a canonical library place for this kind of logic
    private static OutputStream wrapOutputStream(OutputStream outputStream) throws IOException {

        switch (GZTYPE) {
            case 0:
                // no compression
                return outputStream;
            case 1:
                // LZF
                return new LZFOutputStream(outputStream);
            case 2:
                // Snappy
                return new SnappyOutputStream(outputStream);
            default:
                throw new RuntimeException("Unknown compression type: " + GZTYPE);
        }
    }

    // TODO disk cleanup
    @Override
    public void sendComplete() {
        /** optimization for when buffer hasn't yet spilled */
        if (chunk == 0) {
            Arrays.sort(buffer, 0, bufferIndex, comparator);
            for (int i = 0; i < bufferIndex; i++) {
                if (!queryStatusObserver.queryCompleted) {
                    getNext().send(buffer[i]);
                } else {
                    break;
                }
            }
            cleanup();
            super.sendComplete();
            return;
        }
        if (!queryStatusObserver.queryCompleted) {
            dumpBufferToMFM();
        } else {
            cleanup();
            super.sendComplete();
            return;
        }
        int level = 0;
        /** progressively compact levels until only one chunk is emitted in a merge */
        if (chunk > CHUNK_MERGES) {
            while (mergeLevel(level++) > CHUNK_MERGES) {
                if (queryStatusObserver.queryCompleted) {
                    break;
                }
            }
        }
        if (queryStatusObserver.queryCompleted) {
            cleanup();
            super.sendComplete();
            return;
        }
        /** stream results from last round of merging */
        SortedSource sortedSource = new SortedSource(level, 0, CHUNK_MERGES);
        Bundle next = null;
        int bundles = 0;
        while ((next = sortedSource.next()) != null) {
            if (!queryStatusObserver.queryCompleted) {
                getNext().send(next);
                bundles++;
            } else {
                break;
            }
        }
        log.debug("finish read from level={} chunk=0 bundles={}", level, bundles);
        cleanup();
        super.sendComplete();
    }

    /**
     * merge chunks in a level and return the number of resulting chunks
     */
    private int mergeLevel(int level) {
        int chunkOut = 0;
        int levelOut = level + 1;
        int chunk = 0;
        int merges = 0;
        int bundles = 0;
        while (true) {
            SortedSource sortedSource = new SortedSource(level, chunk, CHUNK_MERGES);
            int readers = sortedSource.getReaderCount();
            log.debug("SourceSource({},{},{}) = {}", level, chunk, CHUNK_MERGES, readers);
            if (readers == 0) {
                log.debug("mergeLevel({})={} chunkIn={} bundles={}", level, merges, chunk, bundles);
                return merges;
            }
            chunk += readers;
            merges++;
            try {
                MuxFile meta = mfm.openFile("l" + levelOut + "-c" + (chunkOut++), true);
                log.debug(" output to level={} chunk={}", levelOut, chunkOut - 1);
                try (OutputStream out = wrapOutputStream(meta.append());
                     DataChannelWriter writer = new DataChannelWriter(out);) {
                    Bundle next = null;
                    while ((next = sortedSource.next()) != null) {
                        writer.write(next);
                        bundles++;
                    }
                }
                meta.sync();
                log.debug(" output bundles={}", bundles);
            } catch (Exception ex) {
                sortedSource.tryClean();
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private static int longCompare(ValueObject s1, ValueObject s2) {
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return ValueUtil.asNumberOrParseLong(s1, 10).asLong().getLong().compareTo(ValueUtil.asNumberOrParseLong(s2, 10).asLong().getLong());
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private static int doubleCompare(ValueObject s1, ValueObject s2) {
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return ValueUtil.asNumberOrParseDouble(s1).asDouble().getDouble().compareTo(ValueUtil.asNumberOrParseDouble(s2).asDouble().getDouble());
    }

    /**
     * @param s1
     * @param s2
     * @return
     */
    private static int stringCompare(ValueObject s1, ValueObject s2) {
        if (s1 == OpPivot.MIN || s2 == OpPivot.MAX) {
            return -1;
        }
        if (s1 == OpPivot.MAX || s2 == OpPivot.MIN) {
            return 1;
        }
        if (s1 == s2) {
            return 0;
        }
        if (s1 == null) {
            return 1;
        }
        if (s2 == null) {
            return -1;
        }
        return s1.toString().compareTo(s2.toString());
    }

    private static InputStream wrapInputStream(InputStream inputStream) throws IOException {

        switch (GZTYPE) {
            case 0:
                // no compression
                return inputStream;
            case 1:
                // LZF
                return new LZFInputStream(inputStream);
            case 2:
                // Snappy
                return new SnappyInputStream(inputStream);
            default:
                throw new RuntimeException("Unknown compression type: " + GZTYPE);
        }
    }

    private class BundleComparator implements Comparator<Bundle> {

        private BundleField columns[];

        @Override
        public int compare(Bundle o1, Bundle o2) {
            if (columns == null) {
                columns = new BundleColumnBinder(o1, cols).getFields();
            }
            int delta = 0;
            for (int i = 0; i < columns.length; i++) {
                BundleField col = columns[i];
                if (delta == 0) {
                    switch (type[i]) {
                        case 'i': // int
                        case 'l': // long
                        case 'n': // legacy "number"
                            delta = longCompare(o1.getValue(col), o2.getValue(col));
                            break;
                        case 'd': // double
                        case 'f': // float
                            delta = doubleCompare(o1.getValue(col), o2.getValue(col));
                            break;
                        case 's': // string
                        default:
                            delta = stringCompare(o1.getValue(col), o2.getValue(col));
                            break;
                    }

                    if (dir[i] == 'd') {
                        delta = -delta;
                    }
                } else {
                    break;
                }
                log.debug("compare i={} col={} o1={} o2={} type={} delta={} o1v={} o2v={}",
                        i, col, o1, o2, type[i], delta, o1.getValue(col), o2.getValue(col));
            }
            return delta;
        }
    }

    private class SortedSource {

        private final TreeSet<SourceBundle> sorted = new TreeSet<>(new SourceBundleComparator());
        private final LinkedList<DataChannelReader> readers = new LinkedList<>();
        private long bundleCounter = 0L;

        SortedSource(final int level, int chunk, int count) {
            while (count-- > 0) {
                try {
                    // TODO figure out how to delete these files after consuming them to keep the index small in mem
                    MuxFile meta = mfm.openFile("l" + level + "-c" + (chunk++), false);
                    DataChannelReader reader = new DataChannelReader(factory, wrapInputStream(meta.read(0)));
                    Bundle next = null;
                    try {
                        next = reader.read();
                    } catch (Exception ignored) {
                    }
                    if (next != null) {
                        log.debug("source source open level={} chunk={} next={} meta={}",
                                level, chunk - 1, next, meta);
                        readers.add(reader);
                        sorted.add(new SourceBundle(next, reader, bundleCounter++));
                    } else {
                        reader.close();
                    }
                } catch (IOException e) {
                    log.debug("swallowing mystery io exception", e);
                    break;
                }
            }
            log.debug("SortedSource seeded with {} entries", sorted.size());
        }

        public void tryClean() {
            try {
                for (DataChannelReader reader : readers) {
                    reader.close();
                }
            } catch (Exception ex) {
                log.warn("exception while trying to close disk sort readers", ex);
            }
        }

        public int getReaderCount() {
            return readers.size();
        }

        /**
         * each call to next re-populates the tree from the same source
         */
        public Bundle next() {
            if (!sorted.isEmpty()) {
                Iterator<SourceBundle> iter = sorted.iterator();
                SourceBundle nextOrdered = iter.next();
                iter.remove();
                try {
                    Bundle nextFromSource = nextOrdered.reader.read();
                    if (nextFromSource != null) {
                        sorted.add(new SourceBundle(nextFromSource, nextOrdered.reader, bundleCounter++));
                    }
                } catch (EOFException ignored) {
                    log.debug("closing source on EOF size={}", sorted.size());
                    try {
                        nextOrdered.reader.close();
                    } catch (Exception ex2) {
                        // ignore
                    }
                } catch (Exception ex) {
                    log.warn("swallowing mystery exception", ex);
                }
                return nextOrdered.bundle;
            }
            return null;
        }

        private class SourceBundle {

            public final Bundle bundle;
            public final DataChannelReader reader;
            public final long uniqueID;

            SourceBundle(Bundle bundle, DataChannelReader reader, long uniqueID) {
                this.bundle = bundle;
                this.reader = reader;
                this.uniqueID = uniqueID;
            }
        }

        private class SourceBundleComparator implements Comparator<SourceBundle> {

            @Override
            public int compare(SourceBundle o1, SourceBundle o2) {
                int comp = comparatorSS.compare(o1.bundle, o2.bundle);
                if (comp == 0) {
                    // The following two comparisons are necessary to avoid overflow or underflow
                    // that can result from using (o1.uniqueID - o2.uniqueID).
                    // When support is dropped for Java 6, these comparisons can be replaced with
                    /// Long.compare(o1.uniqueID, o2.uniqueID) (introduced in Java 7).
                    if (o1.uniqueID > o2.uniqueID) {
                        return 1;
                    } else if (o1.uniqueID == o2.uniqueID) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    return comp;
                }
            }
        }
    }
}
