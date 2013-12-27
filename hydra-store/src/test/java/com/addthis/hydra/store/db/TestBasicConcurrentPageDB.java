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

import com.addthis.basis.test.SlowTest;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(SlowTest.class)
public class TestBasicConcurrentPageDB extends TestPagedDB {

    @BeforeClass
    public static void oneTimeSetup() {
        keyValueStoreType = 1;
    }

    @Test
    public void fromTop() throws Exception {
        fromTop(createDB(DB.PageDB));
    }

    @Test
    public void fromBottom() throws Exception {
        fromBottom(createDB(DB.PageDB));
    }

    @Test
    public void fromTopAndBottom() throws Exception {
        fromTopAndBottom(createDB(DB.PageDB));
    }

    @Test
    public void reopenTest() throws Exception {
        reopenTest(DB.PageDB);
    }

    @Test
    public void sizeTest() throws Exception {
        sizeTests(DB.PageDB);
    }
}
