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
package com.addthis.hydra.task.source;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.codec.config.Configs.decodeObject;

public class DataSourceFilteredTest {
    private static final Logger log = LoggerFactory.getLogger(DataSourceFilteredTest.class);

    @Test public void basics() throws Exception {
        TaskDataSource filteredSource = decodeObject(
                TaskDataSource.class, "filtered {const: [{a = 5}, {a = 3}, {a = 6}], filter {from: a, require: 3}}");
        filteredSource.init();
        Bundle expected = Bundles.decode("{a = 3}");
        Assert.assertTrue(Bundles.equals(expected, filteredSource.next()));
        filteredSource.close();
    }
}