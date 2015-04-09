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

import java.io.InputStream;

import com.addthis.bundle.core.BundleFactory;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;

/**
 * Specifies the conversion into bundles (this is specific to mesh2).
 * <p>The following factories are available:
 * <ul>
 * <li>{@link ChannelBundleizer channel}</li>
 * <li>{@link ColumnBundleizer column}</li>
 * <li>{@link KVBundleizer kv}</li>
 * </ul>
 *
 * @user-reference
 */
@Pluggable("stream-bundleizer")
public abstract class BundleizerFactory implements Codable {

    public abstract Bundleizer createBundleizer(InputStream input, BundleFactory factory);
}
