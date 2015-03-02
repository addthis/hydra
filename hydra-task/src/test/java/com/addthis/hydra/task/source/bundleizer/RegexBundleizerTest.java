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

public class RegexBundleizerTest {
    private static final Logger log = LoggerFactory.getLogger(RegexBundleizerTest.class);

    @Test public void groupsToFields() throws Exception {
        RegexBundleizer bundleizer = decodeObject(RegexBundleizer.class, "regex = \"(?<digits>\\\\d+) (?<rest>.*)\"");
        Bundle bundle = bundleizer.bundleize(new ListBundle(), "12345 HELLO WORLD");
        AutoField digits = AutoField.newAutoField("digits");
        AutoField rest = AutoField.newAutoField("rest");
        Assert.assertEquals("12345", digits.getString(bundle).get());
        Assert.assertEquals("HELLO WORLD", rest.getString(bundle).get());
    }

    @Test public void fieldsListing() throws Exception {
        RegexBundleizer bundleizer =
                decodeObject(RegexBundleizer.class, "fields = [digits, rest], regex = \"(\\\\d+) (.*)\"");
        Bundle bundle = bundleizer.bundleize(new ListBundle(), "12345 HELLO WORLD");
        AutoField digits = AutoField.newAutoField("digits");
        AutoField rest = AutoField.newAutoField("rest");
        Assert.assertEquals("12345", digits.getString(bundle).get());
        Assert.assertEquals("HELLO WORLD", rest.getString(bundle).get());
    }
}