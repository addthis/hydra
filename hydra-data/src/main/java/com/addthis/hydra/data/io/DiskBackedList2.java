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
package com.addthis.hydra.data.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;

import com.google.common.base.Throwables;

import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of disk-backed list tuned for good sorting performance.
 * Incoming values are split into a number of chunks. Each chunk is small enough
 * to fit entirely within memory.
 *
 * @param <K> a codable object type
 */
public class DiskBackedList2<K> implements List<K> {

    public static interface ItemCodec<K> {

        public K decode(byte[] row) throws IOException;

        public byte[] encode(K row) throws IOException;
    }

    private static final Logger log = LoggerFactory.getLogger(DiskBackedList2.class);

    private ItemCodec<K> codec;
    private List<DBLChunk> chunks;
    private DBLChunk currentChunk;
    private int totalItems;
    private final long maxChunkSizeBytes;
    private final long maxTotalSizeBytes = Parameter.longValue("max.total.query.size.bytes", 10 * 1000 * 1000 * 1000);
    private static final int defaultChunkSizeBytes = Parameter.intValue("default.chunk.size.bytes", 16 * 1000 * 1000);
    private final String filePrefix = "dbl2file-";
    private final String fileSuffix = ".dat";
    private final File directory;

    public DiskBackedList2(ItemCodec<K> codec) throws IOException {
        this(codec, defaultChunkSizeBytes, Files.createTempDir());
    }

    public DiskBackedList2(ItemCodec<K> codec, long maxChunkSizeBytes, File directory) throws IOException {
        this.codec = codec;
        this.maxChunkSizeBytes = maxChunkSizeBytes;
        this.directory = directory;
        this.chunks = new ArrayList<>();
        this.currentChunk = addChunk();
        this.totalItems = 0;
    }

    public void setCodec(ItemCodec<K> codec) {
        this.codec = codec;
    }

    @Override
    protected void finalize() {
        chunks.clear();
        currentChunk = null;
    }

    public String toString() {
        return "DBL: rows=" + totalItems + ", numChunks=" + chunks.size() + ", maxChunkSize=" + maxChunkSizeBytes;
    }

    // Which chunk is this element on and where is the element in that list?
    private Pair<Integer, Integer> indicesForElement(int elementIndex) {
        int chunkIndex = 0;
        int remaining = elementIndex;
        for (DBLChunk chunk : chunks) {
            int numEntries = chunk.getNumEntries();
            if (remaining >= numEntries) {
                remaining -= numEntries;
                chunkIndex += 1;
            } else {
                break;
            }
        }
        return Pair.of(chunkIndex, remaining);
    }

    private void loadChunk(int chunkIndex) throws IOException {
        if (currentChunk == null) {
            loadChunkFully(chunkIndex);
        } else if (currentChunk.getIndex() != chunkIndex) {
            currentChunk.saveToFile();
            currentChunk.clear();
            loadChunkFully(chunkIndex);
        }
    }

    private void installChunk(int index, DBLChunk chunk) {
        try {
            while (index >= chunks.size()) {
                chunks.add(new DBLChunk(chunks.size(), directory));
            }
            chunks.set(index, chunk);
            totalItems += chunk.getNumEntries();
            currentChunk = null;
        } catch (IOException io) {
            log.warn("exception during install chunk: ", io);
        }
    }

    @Override
    public boolean add(K element) {
        try {
            loadChunk(chunks.size() - 1);
            if (!currentChunk.hasRoom()) {
                currentChunk = addChunk();
            }
            currentChunk.store(element);
            totalItems++;
            return true;
        } catch (IOException ex) {
            log.warn("[disk.backed.list] exception while adding " + element, ex);
            }
        return false;
    }

    @Override
    /**
     * This function adds an element to a particular location.
     * TODO does not presently respect maxChunkSize
     */
    public void add(int elementIndex, K element) {
        Pair<Integer, Integer> indices = indicesForElement(elementIndex);
        try {
            loadChunk(indices.getLeft());
            currentChunk.store(indices.getRight(), element);
            totalItems++;
        } catch (IOException io) {
            log.warn("[disk.backed.list] exception while adding " + element, io);
            }
    }

    @Override
    public boolean addAll(Collection<? extends K> c) {
        for (K k : c) {
            add(k);
        }
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends K> c) {
        for (K k : c) {
            add(index++, k);
        }
        return true;
    }

    @Override
    public void clear() {
        for (DBLChunk chunk : chunks) {
            chunk.deleteFile();
        }
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Iterator<?> iter = c.iterator(); iter.hasNext();) {
            if (!contains(iter.next())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public K get(int elementIndex) {
        try {
            Pair<Integer, Integer> indices = indicesForElement(elementIndex);
            loadChunk(indices.getLeft());
            return currentChunk.get(indices.getRight());
        } catch (IOException ex) {
            log.warn("[disk.backed.list] exception while getting element " + elementIndex, ex);
            return null;
        }
    }

    @Override
    public int indexOf(Object o) {
        int index = 0;
        //noinspection unchecked
        for (K elt : (Iterable<K>) iterator()) {
            if (elt.equals(o)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        return chunks.isEmpty();
    }

    @Override
    public Iterator<K> iterator() {
        return listIterator();
    }

    @Override
    public int lastIndexOf(Object o) {
        int index = 0;
        int found = -1;
        //noinspection unchecked
        for (K elt : (Iterable<K>) iterator()) {
            if (elt.equals(o)) {
                found = index;
            }
            index++;
        }
        return found;
    }

    @Override
    public ListIterator<K> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<K> listIterator(final int ind) {
        try {
            if (currentChunk != null) {
                currentChunk.saveToFile();
            }
        } catch (IOException ex) {
            log.warn("[disk.backed.list] exception during saving", ex);
            return null;
        }
        return new ListIterator<K>() {

            private int index = ind;

            @Override
            public boolean hasNext() {
                return index < totalItems;
            }

            @Override
            public K next() {
                return get(index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(K e) {
                DiskBackedList2.this.add(e);
            }

            @Override
            public boolean hasPrevious() {
                return index > 0;
            }

            @Override
            public int nextIndex() {
                return index;
            }

            @Override
            public K previous() {
                return get(index--);
            }

            @Override
            public int previousIndex() {
                return index - 1;
            }

            @Override
            public void set(K e) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean remove(Object o) {
        return remove(indexOf(o)) != null;
    }

    @Override
    public K remove(int elementIndex) {
        Pair<Integer, Integer> indices = indicesForElement(elementIndex);
        try {
            loadChunk(indices.getLeft());
        } catch (IOException io) {
            log.warn("[disk.backed.list] io exception during remove operation", io);
            return null;
        }
        K elt = currentChunk.remove(indices.getRight().intValue());
        totalItems -= 1;
        return elt;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean success = true;
        for (Iterator<?> iter = c.iterator(); iter.hasNext();) {
            if (!remove(iter.next())) {
                success = false;
            }
        }
        return success;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K set(int elementIndex, K element) {
        Pair<Integer, Integer> indices = indicesForElement(elementIndex);
        try {
            loadChunk(indices.getLeft());
        } catch (IOException io) {
            log.warn("[disk.backed.list] io exception during remove operation", io);
            }
        return currentChunk.set(indices.getRight(), element);
    }

    @Override
    public int size() {
        return totalItems;
    }

    @Override
    public List<K> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * clean up and close
     */
    public void close() throws IOException {
        if (currentChunk != null) {
            currentChunk.saveToFile();
            currentChunk = null;
        }
    }

    /**
     * Sort the collection of elements using a standard external sort algorithm: sort each chunk of elements, then
     * merge the chunks into a new list, then switch to the new list.
     */
    public void sort(final Comparator<? super K> comp) {
        try {
            // Sort each chunk. Done if there is only one chunk.
            sortEachChunk(comp);
            if (chunks.size() <= 1) {
                return;
            }
            Comparator<Pair<K, Integer>> pairComp = new Comparator<Pair<K, Integer>>() {
                @Override
                public int compare(Pair<K, Integer> e1, Pair<K, Integer> e2) {
                    return comp.compare(e1.getLeft(), e2.getLeft());
                }
            };
            // This heap stores the lowest remaining value from each chunk
            PriorityQueue<Pair<K, Integer>> heap = new PriorityQueue<>(chunks.size(), pairComp);
            ArrayList<Iterator> iterators = new ArrayList<>(chunks.size());

            // Initialize the heap with one value per chunk
            close();
            for (int i = 0; i < chunks.size(); i++) {
                Iterator<K> it = chunks.get(i).getChunkIterator();
                iterators.add(i, it);
                if (it.hasNext()) {
                    K elt = it.next();
                    if (elt != null) {
                        heap.add(Pair.of(elt, i));
                    }
                }
            }
            // Make a new disk backed list to store sorted values.
            // When the number of chunks is large, the size of the output buffer needs to shrink to make up for the extra mem usage
            long storageMaxChunkSize = maxChunkSizeBytes / (1 + chunks.size() / 20);
            DiskBackedList2<K> storage = new DiskBackedList2<>(codec, storageMaxChunkSize, directory);

            // Repeatedly pull the smallest element from the heap
            while (!heap.isEmpty()) {
                Pair<K, Integer> leastElt = heap.poll();
                storage.add(leastElt.getLeft());
                @SuppressWarnings({"unchecked"})
                Iterator<K> polledIterator = iterators.get(leastElt.getRight());
                if (polledIterator.hasNext()) {
                    heap.add(Pair.of(polledIterator.next(), leastElt.getRight()));
                }
            }

            // Switch to the storage dbl's chunks
            storage.close();
            chunks = storage.getChunks();
            currentChunk = null;
        } catch (IOException io) {
            throw Throwables.propagate(io);
        }
    }

    // This function is used to switch to the sorted values after a sort
    private List<DBLChunk> getChunks() {
        return chunks;
    }

    private void sortEachChunk(final Comparator<? super K> comp) throws IOException {
        int startChunkIndex = currentChunk != null ? currentChunk.getIndex() : 0;
        int numChunks = chunks.size();
        for (int i = 0; i < numChunks; i++) {
            int chunkIndex = (i + startChunkIndex) % numChunks;
            loadChunk(chunkIndex);
            Collections.sort(currentChunk, comp);
        }
    }

    public void addEncodedData(Iterator<byte[]> stream) throws Exception {
        while (stream.hasNext()) {
            byte[] bytes = stream.next();
            add(codec.decode(bytes));
        }
    }

    public Iterator<byte[]> getEncodedData() {
        return new Iterator<byte[]>() {
            final Iterator<K> iter = listIterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public byte[] next() {
                try {
                    return codec.encode(iter.next());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private DBLChunk addChunk() throws IOException {
        if (currentChunk != null) {
            currentChunk.saveToFile();
            currentChunk.clear();
        }
        int index = chunks.size();
        if (maxTotalSizeBytes > 0 && index * maxChunkSizeBytes > maxTotalSizeBytes) {
            log.warn("[disk.backed.list] disk space limit exceeded: " + this.toString());
            throw new RuntimeException("Query exceeded the disk space limit on sort file storage. (Number of rows at time of failure: " +
                                       totalItems + "). Try using query operations to reduce the size and number of rows being sorted, so as not to stress the QueryMaster.");
        }
        DBLChunk chunk = new DBLChunk(index, directory);
        chunks.add(chunk);
        return chunk;
    }

    /**
     * Load the specified chunk from the disk. By design, only currentChunk can be loaded into memory, ensuring that
     * only one DBLChunk is loaded at any given time.
     */
    private void loadChunkFully(int index) throws IOException {
        currentChunk = chunks.get(index);
        currentChunk.resetNumEntries();
        currentChunk.readFromFile();
    }


    public void loadAllChunksFromDirectory() throws IOException {
        if (directory != null && directory.isDirectory()) {
            for (int i = 0; i < directory.listFiles().length; i++) {
                File file = new File(directory, filePrefix + Integer.toString(i) + fileSuffix);
                if (file.exists()) {
                    DBLChunk chunk = new DBLChunk(i, directory);
                    chunk.readFromFile();
                    installChunk(i, chunk);
                }
            }
        }
    }

    public void saveAllChunksToDirectory() throws IOException {
        if (directory != null && directory.isDirectory()) {
            currentChunk.saveToFile();
        }
    }

    /**
     * An ArrayList of entries that can be loaded and saved to disk.
     * Each DBLChunk is small enough that it can be loaded entirely into memory.
     */
    private class DBLChunk extends ArrayList<K> {

        private final int index;
        private int numEntries;
        private long availableBytes;
        private File file;

        public String makeFileName() {
            return filePrefix + index;
        }

        public DBLChunk(int index, File directory) throws IOException {
            this.index = index;
            this.numEntries = 0;
            this.availableBytes = maxChunkSizeBytes;
            file = new File(directory, makeFileName() + fileSuffix);
        }

        public int getIndex() {
            return index;
        }

        public boolean hasRoom() {
            return availableBytes > 0;
        }

        public int getNumEntries() {
            return numEntries;
        }

        public boolean store(K element) {
            return store(this.size(), element);
        }

        public boolean store(int elementIndex, K element) {
            long totalBytes = 4 + MemoryCounter.estimateSize(element);
            availableBytes -= totalBytes;
            this.add(elementIndex, element);
            numEntries += 1;
            return true;
        }

        public void saveToFile() throws IOException {
            FileOutputStream fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(this.size());
            for (K val : this) {
                byte[] valBytes = codec.encode(val);
                if (log.isDebugEnabled()) log.debug("stf: encoding " + val + " as " + valBytes.length + " to " + file);
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
            dos.close();
        }

        public boolean deleteFile() {
            return file != null && file.delete();
        }

        public void readFromFile() throws IOException {
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);
            DataInputStream dis = new DataInputStream(bis);
            int newNum = dis.readInt();
            for (int i = 0; i < newNum; i++) {
                int newLen = dis.readInt();
                byte[] bytes = Bytes.readBytes(dis, newLen);
                if (bytes == null || bytes.length == 0 || newLen == 0) {
                    log.warn("read null/0 bytes @ i=" + i + " of " + newNum + " for index=" + index + " file=" + file);
                } else {
                    if (log.isDebugEnabled()) log.debug("rff: decoding " + bytes.length + " bytes from " + file);
                    K tv = codec.decode(bytes);
                    this.store(tv);
                }
            }
            dis.close();
        }

        public void resetNumEntries() {
            numEntries = 0;
        }

        public Iterator<K> getChunkIterator() throws IOException {
            return new ListIterator<K>() {
                final FileInputStream fis = new FileInputStream(file);
                final BufferedInputStream bis = new BufferedInputStream(fis);
                final DataInputStream dis = new DataInputStream(bis);
                int itemsRemaining = dis.readInt();

                {
                    if (log.isDebugEnabled()) log.debug("iter: items=" + itemsRemaining + " file=" + file);
                }

                @Override
                public boolean hasNext() {
                    return itemsRemaining > 0;
                }

                @Override
                public K next() {
                    try {
                        int eltLength = dis.readInt();
                        byte[] bytes = Bytes.readBytes(dis, eltLength);
                        itemsRemaining--;
                        if (itemsRemaining == 0) {
                            dis.close();
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("iter: decoding " + bytes.length + " bytes remain=" + itemsRemaining + " from " + file);
                        }
                        return codec.decode(bytes);
                    } catch (IOException io) {
                        log.warn("[disk.backed.list] io exception during chunk iteration", io);
                        return null;
                    }

                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void add(K e) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean hasPrevious() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int nextIndex() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public K previous() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int previousIndex() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void set(K e) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public File getDirectory() {
        return directory;
    }
}
