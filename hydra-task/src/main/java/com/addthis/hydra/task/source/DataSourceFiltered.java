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

import java.util.NoSuchElementException;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

/**
 * This data source <span class="hydra-summary">applies a filter to an input source</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>source:{
 *     type : "filter"
 *     filter : {
 *       op : "concat",
 *       in : ["YMD", "HMS"],
 *       out : "TIME",
 *       join : " ",
 *     },
 *     stream : {
 *       type : "stream2",
 *       hash : true,
 *       source : {
 *         startDate : "{{now-2}}",
 *         endDate : "{{now}}",
 *         dateFormat : "YYMMddHH",
 *         hosts : [
 *           {host : "san1.local", port : 1337},
 *         ],
 *         files : [{
 *           dir : "logs/%[code:1234]%/{YY}/{M}/{D}/",
 *           match : [".*log_%[code]%.*{YY}{M}{D}{H}00-.*gz"],
 *         }],
 *       },
 *       factory : {
 *         type : "column",
 *         columns : [
 *           "YMD",
 *           "HMS",
 *           ...
 *         ]
 *       }
 *     }
 *   }</pre>
 *
 * @user-reference
 * @hydra-name filter
 */
public class DataSourceFiltered extends TaskDataSource {

    /**
     * Underlying data source from which data is fetched. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataSource stream;

    /**
     * Apply this filter to each bundle that is retrieved to the data source. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter filter;

    private Bundle peek;

    @Override
    public void init() {
        if (filter != null) {
            filter.open();
        }
        stream.init();
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public Bundle peek() {
        Bundle tmp = null;
        while (peek == null && (tmp = stream.peek()) != null) {
            if (!filter.filter(tmp)) {
                stream.next();
                continue;
            }
            peek = tmp;
        }
        return peek;
    }

    @Override
    public Bundle next() {
        Bundle ret = null;
        if ((ret = peek()) == null) {
            throw new NoSuchElementException();
        }
        if (stream.next() == null) {
            throw new RuntimeException("next() return null after non-null peek");
        }
        peek = null;
        return ret;
    }
}
