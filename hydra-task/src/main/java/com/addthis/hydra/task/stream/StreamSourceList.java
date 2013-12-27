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

import java.util.LinkedList;


public class StreamSourceList implements StreamFileSource {

    public StreamSourceList() {
        this.files = new LinkedList<StreamFile>();
    }

    private final LinkedList<StreamFile> files;

    public void addSource(StreamFile file) {
        files.add(file);
    }

    @Override
    public StreamFile nextSource() {
        return files.size() > 0 ? files.removeFirst() : null;
    }

}
