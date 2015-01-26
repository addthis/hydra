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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;


/**
 * chops a newline separated stream into strings to be bundleized
 */
public abstract class NewlineBundleizer extends BundleizerFactory {

    @FieldConfig(codable = true)
    private ValueFilter lineFilter;

    @Override
    public void open() {
        if (lineFilter != null) {
            lineFilter.open();
        }
    }

    @Override
    public Bundleizer createBundleizer(final InputStream inputArg, final BundleFactory factoryArg) {
        return new Bundleizer() {
            private final BufferedReader reader = new BufferedReader(new InputStreamReader(inputArg), 65535);
            private final BundleFactory factory = factoryArg;

            @Override
            public Bundle next() throws IOException {
                String line;
                while (true) {
                    line = reader.readLine();
                    if (line == null) {
                        return null;
                    }
                    if (lineFilter != null) {
                        line = ValueUtil.asNativeString(lineFilter.filter(ValueFactory.create(line)));
                        if (line == null) {
                            continue;
                        }
                    }
                    break;
                }
                return bundleize(factory.createBundle(), line);
            }
        };
    }

    public abstract Bundle bundleize(Bundle next, String line);
}
