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
package com.addthis.hydra.store.util;

/**
 * interface describing basic bloom filter usage.
 *
 * @param <K>
 */
public interface SeenFilter<K> {

    /**
     * marks this key as having been seen
     */
    public void setSeen(K o);

    /**
     * returns true if this key has been seen before.  failures are false positives
     */
    public boolean getSeen(K o);

    /**
     * returns true if this key has been seen before the sets the seen bits.
     * more efficient than get then set because it only has to compute the
     * hash and offset bits once.
     */
    public boolean getSetSeen(K o);

    /**
     * merges the seen bits from a compatible filter. and returns a new filter.
     * throws a runtime exception if the filters are imcompatible.
     */
    public SeenFilter<K> merge(SeenFilter<K> filter);

    /**
     * clear seen key marks. all keys become unseen.
     */
    public void clear();
}
