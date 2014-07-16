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

import java.io.EOFException;
import java.io.InputStream;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.io.DataChannelReader;


/**
 * chops a DataChannel stream into Bundles.
 *
 * @hydra-name channel
 */
public class ChannelBundleizer extends BundleizerFactory {

    @Override
    public Bundleizer createBundleizer(final InputStream input, final BundleFactory factory) {
        return new Bundleizer() {
            private final DataChannelReader reader = new DataChannelReader(factory, input);

            @Override
            public Bundle next() throws Exception {
                try {
                    return reader.read();
                } catch (EOFException eof) {
                    return null;
                }
            }
        };
    }
}
