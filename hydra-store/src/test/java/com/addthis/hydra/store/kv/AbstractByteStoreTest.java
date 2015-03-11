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

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.LessFiles;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public abstract class AbstractByteStoreTest {

    private static byte[] createBytes(int input) {
        return String.format("%05d", input).getBytes();
    }

    public abstract ByteStore createByteStore(File dir, String name);

    @Test
    public void testGetPut() {
        File tempDir = null;
        try {
            tempDir = LessFiles.createTempDir();
            ByteStore store = createByteStore(tempDir, "test");
            for (int i = 0; i < 10; i++) {
                byte[] key = createBytes(i);
                byte[] value = createBytes(10 - i);
                store.put(key, value);
            }
            for (int i = 0; i < 10; i++) {
                byte[] key = createBytes(i);
                byte[] expected = createBytes(10 - i);
                byte[] observed = store.get(key);
                assertArrayEquals(expected, observed);
            }
            assertNull(store.get(createBytes(-1)));
            assertNull(store.get(createBytes(10)));
            assertNull(store.get(new String("").getBytes()));
        } catch (IOException ex) {
            fail(ex.getMessage());
        } finally {
            if (tempDir != null) {
                LessFiles.deleteDir(tempDir);
            }
        }
    }


    @Test
    public void testNextHigherValue() {
        File tempDir = null;
        try {
            tempDir = LessFiles.createTempDir();
            ByteStore store = createByteStore(tempDir, "test");
            for (int i = 1; i < 10; i++) {
                byte[] key = createBytes(i);
                byte[] value = createBytes(10 - i);
                store.put(key, value);
            }
            for (int i = 1; i < 9; i++) {
                byte[] key = createBytes(i);
                byte[] expected = createBytes(i + 1);
                byte[] observed = store.higherKey(key);
                assertArrayEquals(expected, observed);
            }
            assertArrayEquals(createBytes(1), store.higherKey(createBytes(0)));
            assertNull(store.higherKey(createBytes(9)));
            assertNull(store.higherKey(createBytes(10)));
        } catch (IOException ex) {
            fail(ex.getMessage());
        } finally {
            if (tempDir != null) {
                LessFiles.deleteDir(tempDir);
            }
        }
    }

}
