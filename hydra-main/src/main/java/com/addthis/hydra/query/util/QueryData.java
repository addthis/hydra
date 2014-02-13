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
package com.addthis.hydra.query.util;

import java.util.Map;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.service.file.FileReference;

public class QueryData implements Comparable<QueryData> {

    public final ChannelMaster channelMaster;
    public final FileReference fileReference;
    public final Map<String, String> queryOptions;
    public final int taskId;
    public final String jobId;
    public HostEntryInfo hostEntryInfo;

    public QueryData(ChannelMaster channelMaster, FileReference fileReference, Map<String, String> queryOptions, String jobId, int taskId) {
        this.channelMaster = channelMaster;
        this.fileReference = fileReference;
        this.queryOptions = queryOptions;
        this.taskId = taskId;
        this.jobId = jobId;
        this.hostEntryInfo = new HostEntryInfo(fileReference.getHostUUID(), taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryData queryData = (QueryData) o;

        if (taskId != queryData.taskId) return false;
        if (jobId != null ? !jobId.equals(queryData.jobId) : queryData.jobId != null) {
            return false;
        }
        if (fileReference != null ? !fileReference.equals(queryData.fileReference) : queryData.fileReference != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = fileReference != null ? fileReference.hashCode() : 0;
        result = 31 * result + taskId;
        return result;
    }

    @Override
    public int compareTo(QueryData queryData) {
        return hostEntryInfo.getHostName().compareTo(queryData.hostEntryInfo.getHostName());
    }
}
