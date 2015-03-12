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

import io.netty.util.ResourceLeak;

/**
 * Collection who contents can either be read exactly once or
 * whose contents can be released
 * @param <T>
 */
@NotThreadSafe
public class ReadOnceListLeakDetection<T> implements ReadOnceList<T> {

    @Nonnull
    protected ResourceLeak leak;

    @Nonnull
    protected ReadOnceList<T> list;

    public ReadOnceListLeakDetection(@Nonnull ResourceLeak leak, @Nonnull ReadOnceList list) {
        this.leak = leak;
        this.list = list;
    }

    @Override
    public Iterator<T> iterator() {
        leak.close();
        return list.iterator();
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        leak.close();
        list.forEach(action);
    }

    @Override
    public Spliterator<T> spliterator() {
        leak.close();
        return list.spliterator();
    }

    /**
     * Returns either null or the first element of the list.
     * Marks the entire list as read.
     */
    @Override
    public T head() {
        leak.close();
        return list.head();
    }

    @Override public List<T> consume() {
        leak.close();
        return list.consume();
    }

    @Override
    public void addAll(ReadOnceList<T> elements) {
        leak.record();
        list.addAll(elements);
    }

    @Override public void addAll(List<T> elements) {
        leak.record();
        list.addAll(elements);
    }

    @Override
    public void add(T element) {
        leak.record();
        list.add(element);
    }

    /**
     * A list that has been read cannot be released. A list cannot be released more than once.
     */
    @Override
    public void release() {
        leak.close();
        list.release();
    }

    /**
     * Returns number of elements in the list. Does not affect the read or release state.
     */
    @Override
    public int size() {
        leak.record();
        return list.size();
    }

    /**
     * Returns whether the list is empty. Does not affect the read or release state.
     */
    @Override
    public boolean isEmpty() {
        leak.record();
        return list.isEmpty();
    }

    /**
     * Returns true if the list has been read.
     */
    @Override
    public boolean isRead() {
        leak.record();
        return list.isRead();
    }

    /**
     * Returns true if the list has been released.
     */
    @Override
    public boolean isReleased() {
        leak.record();
        return list.isReleased();
    }
}
