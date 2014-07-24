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

package com.addthis.hydra.store.kv;

import java.util.Iterator;
import java.util.Map;

public interface ByteStore {

    public boolean hasKey(byte[] key);

    public boolean isReadOnly();

    public byte[] firstKey();

    public byte[] lastKey();

    /**
     * delete selected entry and return the lexicographically previous key
     */
    public byte[] delete(byte[] key);

    public void put(byte[] key, byte[] val);

    public byte[] get(byte[] key);

    /**
     * return the least key strictly greater than the given key, or null if there is no such key.
     */
    public byte[] higherKey(byte[] key);

    /**
     * returns the greatest key strictly less than the given key, or null if there is no such key.
     */
    public byte[] lowerKey(byte[] key);

    /**
     * return greatest key less than or equal to 'key'
     */
    public byte[] floorKey(byte[] key);

    /**
     * return (key,value) with greatest key less than or equal to 'key'
     */
    public Map.Entry<byte[], byte[]> floorEntry(byte[] key);

    /**
     * return first entry in db
     */
    public byte[] firstEntry();

    public Iterator<byte[]> keyIterator(byte[] start);

    public void close();

    /**
     * Close the database.
     *
     * @param cleanLog if true then wait for the BerkeleyDB clean thread to finish.
     */
    public void close(boolean cleanLog);

    /**
     * This should be should be considered a fairly expensive operation.
     **/
    long count();
}

