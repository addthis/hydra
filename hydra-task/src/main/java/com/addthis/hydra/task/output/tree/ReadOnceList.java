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

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Sequence of elements who contents can either be read
 * exactly once or whose contents can be released exactly once.
 * Contents cannot be both read and released.
 *
 * The {@link Iterable} methods and the {@link #head} and {@link #consume}
 * methods are considered to be reading methods. The {@link #release}
 * method is considered to be a release method. All insertions must
 * occur prior to reading or releasing.
 *
 * An {@code IllegalStateException} is thrown if these constraints are violated.
 */
@NotThreadSafe
public interface ReadOnceList<T> extends Iterable<T> {

    @Override public Iterator<T> iterator();

    @Override public void forEach(Consumer<? super T> action);

    @Override public Spliterator<T> spliterator();

    /**
     * Returns either null or the first element of the list.
     * Marks the entire list as read.
     */
    public T head();

    public List<T> consume();

    public void addAll(ReadOnceList<T> elements);

    public void addAll(List<T> elements);

    public void add(T element);

    /**
     * A list that has been read cannot be released. A list cannot be released more than once.
     */
    public void release();

    /**
     * Returns number of elements in the list. Does not affect the read or release state.
     */
    public int size();

    /**
     * Returns whether the list is empty. Does not affect the read or release state.
     */
    public boolean isEmpty();

    /**
     * Returns true if the list has been read.
     */
    public boolean isRead();

    /**
     * Returns true if the list has been released.
     */
    public boolean isReleased();



}
