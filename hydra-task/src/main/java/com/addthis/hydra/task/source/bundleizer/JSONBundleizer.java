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

import java.io.IOException;
import java.io.UncheckedIOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.codec.jackson.Jackson;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class JSONBundleizer extends NewlineBundleizer {

    // Allows json to be parsed into bundles, not using the "type" field to imply the bundle implementation.  In
    // particular, this is needed for parsing apache logs in json format.
    public final Config overrides = ConfigFactory.parseString("plugins.bundle._field: _notypefield");

    // Slightly inefficient, in that Bundles.decode doesn't support modifying a bundle in place, nor is it able to reuse
    // bundle formats/fields.  Instead we need to merge the resulting bundle into the input bundle.
    @Override
    public Bundle bundleize(Bundle next, String line) {
        try {
            Bundle bundle = Jackson.defaultCodec().withOverrides(overrides).decodeObject(Bundle.class, line);
            Bundles.addAll(bundle, next, true);
            return next;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
