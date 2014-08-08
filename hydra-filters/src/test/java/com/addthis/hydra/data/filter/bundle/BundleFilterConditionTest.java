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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.config.Configs;
import com.addthis.codec.json.CodecJSON;

import org.junit.Test;

public class BundleFilterConditionTest {

    @Test public void simpleRun() {
        Bundle bundle = new ListBundle();
        BundleFilterCondition filter = (BundleFilterCondition) Configs.decodeObject(
                BundleFilter.class, "if {true {}}, then {log = PASSED}");
        filter.filter(bundle);
    }

    public static class FilterHolder {
        @FieldConfig public BundleFilter filter;
    }

    @Test public void simpleRunJson() throws Exception {
        String filterDef = "{filter: {op:\"condition\",\n" +
                           "\t\t\t\tifCondition:{op:\"field\", from:\"UID\",filter:{op:\"require\",contains:[\"0000000000000000\"]}},\n" +
                           "\t\t\t\tifDo:{op:\"field\", from:\"TIME\", to:\"SHARD\"},\n" +
                           "                elseDo:{op:\"field\", from:\"UID\", to:\"SHARD\"},\n" +
                           "\t\t\t}}";
        Bundle bundle = new ListBundle();
        FilterHolder filterHolder = CodecJSON.INSTANCE.decode(FilterHolder.class, filterDef.getBytes());
        filterHolder.filter.filter(bundle);
    }
}