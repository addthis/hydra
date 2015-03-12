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
public abstract class ReadOnceList<T> implements Iterable<T> {

    @Nonnull
    private final List<T> data;

    private boolean read;

    private boolean released;

    protected abstract void releaseItem(T data);

    public ReadOnceList() {
        data = new ArrayList<>();
    }

    public ReadOnceList(int capacity) {
        data = new ArrayList<>(capacity);
    }

    protected ReadOnceList(List<T> nodes) {
        this.data = nodes;
    }

    @Override public Iterator<T> iterator() {
        if (read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (released) {
            throw new IllegalStateException("attempting to read from list that has been released");
        }
        read = true;
        return data.iterator();
    }

    @Override public void forEach(Consumer<? super T> action) {
        if (read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (released) {
            throw new IllegalStateException("attempting to read from list that has been released");
        }
        read = true;
        data.forEach(action);
    }

    @Override public Spliterator<T> spliterator() {
        if (read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (released) {
            throw new IllegalStateException("attempting to read from list that has been released");
        }
        read = true;
        return data.spliterator();
    }

    /**
     * Returns either null or the first element of the list.
     * Marks the entire list as read.
     */
    public T head() {
        if (read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (released) {
            throw new IllegalStateException("attempting to read from list that has been released");
        }
        read = true;
        if (data.size() == 0) {
            return null;
        } else {
            return data.get(0);
        }
    }

    public void addAll(ReadOnceList<T> elements) {
        if (read) {
            throw new IllegalStateException("attempting to insert into a list that has been read");
        }
        if (released) {
            throw new IllegalStateException("attempting to insert into a list that has been released");
        }
        if (elements.read) {
            throw new IllegalStateException("attempting second read from list");
        }
        if (elements.released) {
            throw new IllegalStateException("attempting read from a list that has been released");
        }
        data.addAll(elements.data);
        elements.read = true;
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
        forEach((x) -> releaseItem(x));
        released = true;
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
