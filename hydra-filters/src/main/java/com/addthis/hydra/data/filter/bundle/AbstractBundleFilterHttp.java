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
import com.addthis.bundle.util.AutoField;
import com.addthis.codec.annotations.FieldConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBundleFilterHttp implements BundleFilter {
    private static final Logger log = LoggerFactory.getLogger(AbstractBundleFilterHttp.class);

    @FieldConfig(codable = true) protected CacheConfig cache;
    @FieldConfig(codable = true) protected HttpConfig http;
    @FieldConfig(codable = true) protected String defaultValue;
    @FieldConfig(codable = true) protected boolean trace;
    @FieldConfig(codable = true) protected BundleFilterTemplate url;
    @FieldConfig(codable = true) protected AutoField set;

    public static class CacheConfig {

        @FieldConfig(codable = true)
        public int size = 1000;
        @FieldConfig(codable = true)
        public long   age;
        @FieldConfig(codable = true)
        public String dir;
    }

    public static class HttpConfig {

        @FieldConfig(codable = true)
        public int  timeout      = 60000;
        @FieldConfig(codable = true)
        public int  retries      = 1;
        @FieldConfig(codable = true)
        public long retryTimeout = 1000;
    }

    public static final class ValidationBundleFilterHttp extends AbstractBundleFilterHttp {
        @Override public boolean filter(Bundle row) {
            throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
        }
    }
}
