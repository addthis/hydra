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

package com.addthis.hydra.task.stream.mesh;

import java.util.Comparator;

import com.addthis.hydra.data.filter.value.StringFilter;

public class MeshyStreamFileComparator implements Comparator<MeshyStreamFile> {

    private final StringFilter sortOffSetFilter;

    public MeshyStreamFileComparator(StringFilter sortOffSetFilter) {
        this.sortOffSetFilter = sortOffSetFilter;
    }

    @Override
    public int compare(MeshyStreamFile streamFile1, MeshyStreamFile streamFile2) {

            if (streamFile1.equals(streamFile2)) {
                return 0;
            }
            final String n1 = sortOffSetFilter.filter(streamFile1.name());
            final String n2 = sortOffSetFilter.filter(streamFile2.name());
            int c = n1.compareTo(n2);
            if (c == 0) {
                c = streamFile1.name().compareTo(streamFile2.name());
            }
            if (c == 0) {
                c = Integer.compare(streamFile1.hashCode(), streamFile2.hashCode());
            }
            return c;
        }
}
