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
package com.addthis.hydra.data.filter.closeablebundle;

import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

@Pluggable("closeable bundle filter")
public interface CloseableBundleFilter extends BundleFilter, AutoCloseable {

    /* Filters can implement this if they want to save their data somehow after a job finishes */
    @Override
    void close();
}
