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
package com.addthis.hydra.task.stream;

public class StreamFileUtil {

    /**
     * Get the canonical form of the mesh file name based on the specified sort/path token offsets
     */
    public static String getCanonicalFileReferenceCacheKey(String name, int pathOff, String sortToken, int pathTokenOffset) {
        if (sortToken != null && pathTokenOffset > 0) {
            int pos = 0;
            int off = pathTokenOffset;
            while (off-- > 0 && (pos = name.indexOf(sortToken, pos)) >= 0) {
                pos++;
            }
            if (pos > 0) {
                pathOff += pos;
            }
        }
        return name.substring(pathOff);
    }
}
