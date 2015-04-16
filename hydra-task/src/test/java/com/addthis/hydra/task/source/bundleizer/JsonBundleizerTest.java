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
package com.addthis.hydra.task.source.bundleizer;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.util.AutoField;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.codec.config.Configs.decodeObject;

public class JsonBundleizerTest {
    private static final Logger log = LoggerFactory.getLogger(JsonBundleizerTest.class);

    @Test public void jsonToBundle() throws Exception {
        JSONBundleizer bundleizer = decodeObject(JSONBundleizer.class, "{type: json}");
        String json = "{\"time\": 100, \"uid\":\"freddy\"}";
        Bundle bundle = bundleizer.bundleize(new ListBundle(), json);
        AutoField time = AutoField.newAutoField("time");
        AutoField uid = AutoField.newAutoField("uid");
        Assert.assertEquals(100, time.getInt(bundle).getAsInt());
        Assert.assertEquals("freddy", uid.getString(bundle).get());
    }

    @Test public void ignoreTypeField() throws Exception {
        JSONBundleizer bundleizer = decodeObject(JSONBundleizer.class, "{type: json}");
        String json = "{\"time\": 100, \"uid\":\"freddy\", \"type\": \"notABundleType\"}";
        Bundle bundle = bundleizer.bundleize(new ListBundle(), json);
        AutoField time = AutoField.newAutoField("time");
        AutoField uid = AutoField.newAutoField("uid");
        AutoField type = AutoField.newAutoField("type");
        Assert.assertEquals(100, time.getInt(bundle).getAsInt());
        Assert.assertEquals("freddy", uid.getString(bundle).get());
        Assert.assertEquals("notABundleType", type.getString(bundle).get());
    }
}