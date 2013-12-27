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
package com.addthis.hydra.task.source;

import com.addthis.codec.Codec;
import com.addthis.hydra.task.stream.PersistentStreamFileSource;
import com.addthis.hydra.task.stream.StreamSourceMeshy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This data source <span class="hydra-summary">fetches data from a meshy server</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>{
 *     type : "mesh2",
 *     shardTotal : %[split-shard-total:32]%,
 *     markDir : "mark.db",
 *     mesh : {
 *         startDate : "%[start-date:{{now-2}}]%",
 *         endDate : "%[end-date:{{now}}]%",
 *         meshPort : "%[meshPort:5500]%",dateFormat:"YYMMddHH",
 *         files : ["/job/%[hoover-job]%/&#42;/gold/hoover.out/{Y}{M}{D}/{H}/{{mod}}-&#42;"],
 *     },
 *     format : {
 *         type : "channel",
 *     },
 * }</pre>
 *
 * @user-reference
 * @hydra-name mesh2
 */
public class DataSourceMeshy2 extends AbstractStreamFileDataSource {

    private static final Logger log = LoggerFactory.getLogger(DataSourceMeshy2.class);

    /**
     * Mesh configuration parameters.
     */
    @Codec.Set(codable = true, required = true)
    private StreamSourceMeshy mesh;

    @Override
    public boolean hadMoreData() {
        return mesh != null && mesh.hasMoreData();
    }

    @Override
    protected PersistentStreamFileSource getSource() {
        return mesh;
    }
}
