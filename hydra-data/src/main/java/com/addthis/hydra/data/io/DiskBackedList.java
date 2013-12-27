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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * a disk-backed list in which only the index is stored in memory with
 * objects/bytes kept on disk.  sparse and delete are punted.  when a
 * value is updated, a new object is appended and the old data is left
 * in place.  compaction creates an new backing store and copies over
 * data, deleting the old.
 *
 * @param <K> a codable object type
 */
@Deprecated
public class DiskBackedList<K> implements List<K> {

    /** */
    public static interface ItemCodec<K> {

        public K decode(byte row[]) throws IOException;

        public byte[] encode(K row) throws IOException;
    }

    private static final int headerSize = 64;

    private final LinkedList<DiskBackedListEntry> master = new LinkedList<DiskBackedListEntry>();

    private ItemCodec<K> codec;
    private RandomAccessFile access;
    private AccessFileHandler accessFileHandler;
    private File data;
    private long nextOffset;
    private long firstElement;
    private int cruft;
    private int numSeeks = 0;

    public DiskBackedList(File data, ItemCodec<K> codec) throws IOException {
        this(data, codec, 1000);
    }

    public DiskBackedList(File data, ItemCodec<K> codec, int maxReadBufferSize) throws IOException {
        this.data = data;
        this.codec = codec;
        boolean create = !data.exists() || data.length() == 0;
        access = new RandomAccessFile(data, "rw");
        this.accessFileHandler = new AccessFileHandler(access, maxReadBufferSize);
        if (create) {
            clear();
        } else {
            readHeader();
            System.out.println("importing " + data + " first=" + firstElement + " next=" + nextOffset);
            if (firstElement > 0) {
                DiskBackedListEntry e1 = getEntry(firstElement);
                e1.read();
                master.add(e1);
                while ((e1 = e1.getNext()) != null) {
                    master.add(e1);
                }
            }
        }
    }

    public void setCodec(ItemCodec<K> codec) {
        this.codec = codec;
    }

    @Override
    protected void finalize() {
        if (access != null) {
            System.err.println("finalizing open DiskBackedList rows=" + size() + " size=" + data.length() + " @ " + data);
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean add(K element) {
        add(master.size(), element);
        return true;
    }

    @Override
    public void add(int index, K element) {
        try {
            DiskBackedListEntry added = allocate(element, index < master.size() ? master.get(index) : null);
            master.add(index, added);
            if (index == 0) {
                setFirst(added);
            } else {
                DiskBackedListEntry prev = master.get(index - 1);
                prev.setNext(added);
                prev.update();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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
        master.clear();
        firstElement = 0;
        nextOffset = headerSize;
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
    public K get(int index) {
        return master.get(index).getObjectSafe();
    }

    @Override
    public int indexOf(Object o) {
        int index = 0;
        for (DiskBackedListEntry next : master) {
            if (next.getObjectSafe().equals(o)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        return master.size() == 0;
    }

    @Override
    public Iterator<K> iterator() {
        return listIterator();
    }

    @Override
    public int lastIndexOf(Object o) {
        int index = 0;
        int found = -1;
        for (DiskBackedListEntry next : master) {
            if (next.getObjectSafe().equals(o)) {
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
    public ListIterator<K> listIterator(final int index) {
        return new ListIterator<K>() {

            ListIterator<DiskBackedListEntry> listIter = master.listIterator(index);

            @Override
            public boolean hasNext() {
                return listIter.hasNext();
            }

            @Override
            public K next() {
                return listIter.next().getObjectSafe();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(K e) {
                DiskBackedList.this.add(e);
            }

            @Override
            public boolean hasPrevious() {
                return listIter.hasPrevious();
            }

            @Override
            public int nextIndex() {
                return listIter.nextIndex();
            }

            @Override
            public K previous() {
                return listIter.previous().getObjectSafe();
            }

            @Override
            public int previousIndex() {
                return listIter.previousIndex();
            }

            @Override
            public void set(K e) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean remove(Object o) {
        if (remove(indexOf(o)) != null) {
            cruft++;
            return true;
        }
        return false;
    }

    @Override
    public K remove(int index) {
        if (index >= 0) {
            DiskBackedListEntry e = master.remove(index);
            try {
                DiskBackedListEntry prev = index > 0 ? master.get(index - 1) : null;
                DiskBackedListEntry next = index < master.size() ? master.get(index) : null;
                if (prev != null) {
                    prev.setNext(next);
                    prev.update();
                }
                if (index == 0) {
                    setFirst(next);
                }
                cruft++;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return e.getObjectSafe();
        }
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean success = true;
        for (Iterator<?> iter = c.iterator(); iter.hasNext();) {
            if (!remove(iter.next())) {
                success = false;
            }
        }
        cruft++;
        return success;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K set(int index, K element) {
        DiskBackedListEntry e = master.get(index);
        try {
            DiskBackedListEntry prev = index > 0 ? master.get(index - 1) : null;
            DiskBackedListEntry next = index + 1 < master.size() ? master.get(index + 1) : null;
            DiskBackedListEntry swap = allocate(element, next);
            master.set(index, swap);
            if (prev != null) {
                prev.setNext(swap);
                prev.update();
            }
            if (index == 0) {
                setFirst(swap);
            }
            cruft++;
            return e.getObjectSafe();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int size() {
        return master.size();
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
     * write DiskBackedList magic preamble and pointers
     */
    private void writeHeader() throws IOException {
        access.seek(0);
        access.writeLong(0x12345678L);
        access.writeLong(firstElement);
        access.writeLong(nextOffset);
        numSeeks += 1;
    }

    /**
     * read DiskBackedList magic preamble and pointers
     */
    private void readHeader() throws IOException {
        access.seek(0);
        access.readLong();
        firstElement = access.readLong();
        nextOffset = access.readLong();
        numSeeks += 1;
    }

    /**
     * fix pointers in Entries to align with master list.  run after a sort()
     */
    private void updatePointers() throws IOException {
        DiskBackedListEntry prev = null;
        for (DiskBackedListEntry next : master) {
            if (prev != null) {
                prev.setNext(next);
                prev.update();
            }
            prev = next;
        }
        if (prev != null) {
            prev.setNext(null);
            prev.update();
        }
    }

    private void setFirst(DiskBackedListEntry e) {
        if (e != null) {
            firstElement = e.off;
        } else {
            firstElement = 0;
        }
    }

    /**
     * read Entry from disk based on an offset
     */
    private DiskBackedListEntry getEntry(long off) throws IOException {
        if (off > 0) {
            DiskBackedListEntry e = new DiskBackedListEntry(off);
            e.read();
            return e;
        }
        return null;
    }

    /**
     * allocate a new Entry based on object data and prev/next pointers
     */
    private DiskBackedListEntry allocate(K val, DiskBackedListEntry next) throws Exception {
        return allocate(codec.encode(val), next);
    }

    /**
     * allocate a new Entry based on raw data and prev/next pointers
     */
    private DiskBackedListEntry allocate(byte data[], DiskBackedListEntry next) throws Exception {
        return accessFileHandler.writeToAccess(data, next);
    }

    /**
     * create new DiskBackedList and copy over data compacting out holes
     */
    public void compact() throws Exception {
        if (cruft > 0) {
            File newdata = new File(data.getParentFile(), data.getName().concat(".new"));
            DiskBackedList<K> alt = new DiskBackedList<K>(newdata, codec);
            alt.addEncodedData(getEncodedData());
            clear();
            close();
            data = alt.data;
            nextOffset = alt.nextOffset;
            firstElement = alt.firstElement;
            access = alt.access;
            cruft = 0;
        }
    }

    /**
     * clean up and close
     */
    public void close() throws IOException {
        if (access != null) {
            writeHeader();
            access.close();
            access = null;
        }
    }

    /**
     * perform sort based on object comparisons
     */
    public void sort(final Comparator<K> comp) {
        long starttime = System.currentTimeMillis();
        dumbSort(comp);
        long stoptime = System.currentTimeMillis() - starttime;
        System.out.println("dumbSort took " + stoptime + " ms");
        starttime = System.currentTimeMillis();
        Collections.sort(master, new Comparator<DiskBackedListEntry>() {
            @Override
            public int compare(DiskBackedListEntry o1, DiskBackedListEntry o2) {
                return comp.compare(o1.getObjectSafe(), o2.getObjectSafe());
            }
        });
        try {
            updatePointers();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        stoptime = System.currentTimeMillis() - starttime;
        System.out.println("existing sort took " + stoptime + " ms");
    }

    public <K> void dumbSort(final Comparator<K> comp) {
        List<K> memoryList = new ArrayList<K>();
        Iterator it = this.iterator();
        while (it.hasNext()) {
            memoryList.add((K) it.next());
        }
        Collections.sort(memoryList, comp);
        StringBuilder buf = new StringBuilder();
        for (K thing : memoryList.subList(0, 20)) {
            buf.append(thing);
        }
        System.out.println("Here's dumb sort: " + buf.toString());
    }

    /**
     * consume a list of encoded list elements
     */
    public void addEncodedData(Iterator<byte[]> stream) throws Exception {
        DiskBackedListEntry prev = master.size() > 0 ? master.getLast() : null;
        while (stream.hasNext()) {
            DiskBackedListEntry next = allocate(stream.next(), null);
            if (prev != null) {
                prev.setNext(next);
                prev.update();
            }
            prev = next;
        }
        if (prev != null) {
            prev.update();
        }
    }

    /**
     * produce a list of encoded list elements
     */
    public Iterator<byte[]> getEncodedData() {
        return new Iterator<byte[]>() {
            Iterator<DiskBackedListEntry> iter = master.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public byte[] next() {
                try {
                    return iter.next().getData();
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

    public int getSeeks() {
        return numSeeks;
    }

    private class AccessFileHandler {

        private RandomAccessFile access;
        private int maxReadBufferSize;
        private int maxWriteBufferSize;
        private LinkedHashMap<Long, byte[]> readBuffer = new LinkedHashMap<Long, byte[]>();
        public HashMap<Long, byte[]> writeBuffer = new HashMap<Long, byte[]>();

        public AccessFileHandler(RandomAccessFile access, int maxReadBufferSize) {
            this.access = access;
            this.maxReadBufferSize = maxReadBufferSize;
            this.maxWriteBufferSize = 1000;
        }

        public byte[] getFromAccess(Long offset) throws IOException {
            byte[] result = readBuffer.get(offset);
            if (result == null) {
                access.seek(offset + 8);
                result = new byte[access.readInt()];
                access.readFully(result);
                numSeeks += 1;
            }
            putInReadBuffer(offset, result);
            return result;
        }

        public void putInReadBuffer(Long offset, byte[] data) {
            if (readBuffer.size() >= maxReadBufferSize) {
                readBuffer.clear();
            }
            readBuffer.put(offset, data);
        }

        public DiskBackedListEntry writeToAccess(byte[] bytes, DiskBackedListEntry next) throws IOException {
            putInReadBuffer(nextOffset, bytes);
            DiskBackedListEntry e = new DiskBackedListEntry(nextOffset);
            e.setNext(next);
            e.write(bytes);
            nextOffset += 8 + 4 + bytes.length;
            return e;
        }
    }

    /**
     * pointer bag with reader/write utilities
     */
    private class DiskBackedListEntry {

        private long off;
        private long next;
        private boolean updated;

        private DiskBackedListEntry(long off) {
            this.off = off;
        }

        public void read() throws IOException {
            access.seek(off);
            next = access.readLong();
            numSeeks += 1;
        }

        public void update() throws IOException {
            if (updated) {
                write(null);
                updated = false;
            }
        }

        public void write(byte data[]) throws IOException {
            access.seek(off);
            access.writeLong(next);
            numSeeks += 1;
            if (data != null) {
                access.writeInt(data.length);
                access.write(data);
            }
        }

        public byte[] getData() throws IOException {
            return accessFileHandler.getFromAccess(off);
        }

        @SuppressWarnings("unchecked")
        public K getObject() throws Exception {
            return codec.decode(getData());
        }

        public K getObjectSafe() {
            try {
                return getObject();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public DiskBackedListEntry getNext() throws IOException {
            return getEntry(next);
        }

        public void setNext(DiskBackedListEntry next) {
            if (next == null || next.off != this.next) {
                this.next = next != null ? next.off : 0;
                updated = true;
            }

        }
    }

}
