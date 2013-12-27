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
package com.addthis.hydra.query;

import com.addthis.meshy.service.file.FileReference;

public final class FileReferenceWrapper {

    final FileReference fileReference;
    final int taskId;

    public int getTaskId() {
        return taskId;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    final String name;
    final String host;

    protected FileReferenceWrapper(FileReference fileReference, int taskId) {
        this.fileReference = fileReference;
        this.taskId = taskId;
        this.name = fileReference.name;
        this.host = fileReference.getHostUUID();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileReferenceWrapper that = (FileReferenceWrapper) o;

        if (taskId != that.taskId) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = taskId;
        result = 31 * result + name.hashCode();
        result = 31 * result + (host != null ? host.hashCode() : 0);
        return result;
    }
}
