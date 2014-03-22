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
package com.addthis.hydra.store.db;

import java.io.File;

import com.addthis.basis.test.SlowTest;
import com.addthis.basis.util.Files;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.CachedPagedStore;
import com.addthis.hydra.store.kv.ExternalPagedStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category(SlowTest.class)
public class PageDBTest {

    private static final Logger log = LoggerFactory.getLogger(PageDBTest.class);

    static {
        CachedPagedStore.setDebugLocks(true);
        ExternalPagedStore.setKeyDebugging(true);
    }

    private String tmpDir;

    @Before
    public void before() throws Exception {
        //tmpDir = System.getProperty("java.io.tmpdir") + "/dbtest";
        //Files.deleteDir(new File(tmpDir));
        tmpDir = Files.createTempDir().toString();
        log.warn("[before] tmpDir is :" + tmpDir);
    }

    @After
    public void after() throws Exception {
        log.warn("[after] removing tempdir : " + tmpDir);
        Files.deleteDir(new File(tmpDir));
    }

    @Test
    public void testSimplePutGet() throws Exception {
        PageDB<TestRecord> db = new PageDB<>(Files.initDirectory(tmpDir), TestRecord.class, 100, 100);
        TestRecord record = new TestRecord();
        record.setValue("value");
        assertNull(db.put(new DBKey(0, "key"), record));
        TestRecord record2 = new TestRecord();
        record2.setValue("value2");
        TestRecord previousValue = db.put(new DBKey(0, "key"), record2);
        assertNotNull(previousValue);
        assertEquals("value", previousValue.value);
        TestRecord currentValue = db.get(new DBKey(0, "key"));
        TestRecord currentValue3 = db.get(new DBKey(0, "key"));
        assertNotNull(currentValue);
        assertNotNull(currentValue3);
        assertEquals("value2", currentValue.value);
        db.close();
        db = new PageDB<>(new File(tmpDir), TestRecord.class, 100, 100);
        TestRecord currentValue2 = db.get(new DBKey(0, "key"));
        assertNotNull(currentValue2);
        assertEquals("value2", currentValue2.value);
    }


    public static final class TestRecord implements Codec.BytesCodable {

        @Codec.Set(codable = true)
        private String value;

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public byte[] bytesEncode() {
            return value.getBytes();
        }

        @Override
        public void bytesDecode(byte[] b) {
            value = new String(b);
        }
    }
}
