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

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BdbUtilsTest {

    private static final Logger log = LoggerFactory.getLogger(BdbUtilsTest.class);

    @Test
    public void filterToBdbProps() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("com.addthis.some.prop", "super awesome");
        properties.setProperty("je.sharedCache", "True");
        Properties filteredProperties = BdbUtils.filterToBdbProps(properties);
        Assert.assertEquals("filtered properties should be reduced by one", 1, filteredProperties.size());
        Assert.assertEquals("je.sharedCache should be the one kept", "je.sharedCache",
                            filteredProperties.keys().nextElement());
    }

    @Test
    public void bytesShorthand() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("je.maxMemory", "1750MiB");
        Properties filteredProperties = BdbUtils.filterToBdbProps(properties);
        Assert.assertEquals("byte amounts need to be dumbed down", 1024 * 1024 * 1750,
                            Long.parseLong(filteredProperties.getProperty("je.maxMemory")));
    }
}