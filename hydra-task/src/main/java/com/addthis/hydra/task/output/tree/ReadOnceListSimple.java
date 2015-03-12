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
package com.addthis.hydra.task.output.tree;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Collection who contents can either be read exactly once or
 * whose contents can be released
 * @param <T>
 */
@NotThreadSafe
public class ReadOnceListSimple<T> implements ReadOnceList<T> {

    @Nonnull
    private final List<T> data;

    private final Consumer<T> releaseItem;

    private boolean read;

    private boolean released;

    ReadOnceListSimple(Consumer<T> releaseItem) {
        this.releaseItem = releaseItem;
        this.data = new ArrayList<>();
    }

    ReadOnceListSimple(Consumer<T> releaseItem, int capacity) {
        this.releaseItem = releaseItem;
        this.data = new ArrayList<>(capacity);
    }

    ReadOnceListSimple(Consumer<T> releaseItem, List<T> data) {
        this.releaseItem = releaseItem;
        this.data = data;
    }

    @Override public Iterator<T> iterator() {
        checkAndMarkRead();
        return data.iterator();
    }

    private void checkAndMarkRead() {
        if (read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (released) {
            throw new IllegalStateException("attempting to read from list that has been released");
        }
        read = true;
    }

    @Override public void forEach(Consumer<? super T> action) {
        checkAndMarkRead();
        data.forEach(action);
    }

    @Override public Spliterator<T> spliterator() {
        checkAndMarkRead();
        return data.spliterator();
    }

    /**
     * Returns either null or the first element of the list.
     * Marks the entire list as read.
     */
    public T head() {
        checkAndMarkRead();
        if (data.size() == 0) {
            return null;
        } else {
            return data.get(0);
        }
    }

    public List<T> consume() {
        checkAndMarkRead();
        return data;
    }

    public void addAll(ReadOnceList<T> elements) {
        addAll(elements.consume());
    }

    public void addAll(List<T> elements) {
        if (read) {
            throw new IllegalStateException("attempting to insert into a list that has been read");
        }
        if (released) {
            throw new IllegalStateException("attempting to insert into a list that has been released");
        }
        elements.addAll(elements);
    }

    public void add(T element) {
        if (read) {
            throw new IllegalStateException("attempting to insert into a list that has been read");
        }
        if (released) {
            throw new IllegalStateException("attempting to insert into a list that has been released");
        }
        data.add(element);
    }

    /**
     * A list that has been read cannot be released. A list cannot be released more than once.
     */
    public void release() {
        if (read) {
            throw new IllegalStateException("cannot release a ReadOnceList that has already been read");
        }
        if (released) {
            throw new IllegalStateException("cannot release a ReadOnceList that has already been released");
        }
        released = true;
        forEach((x) -> releaseItem.accept(x));
    }

    /**
     * Returns number of elements in the list. Does not affect the read or release state.
     */
    public int size() {
        return data.size();
    }

    /**
     * Returns whether the list is empty. Does not affect the read or release state.
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Returns true if the list has been read.
     */
    public boolean isRead() {
        return read;
    }

    /**
     * Returns true if the list has been released.
     */
    public boolean isReleased() {
        return released;
    }
}
