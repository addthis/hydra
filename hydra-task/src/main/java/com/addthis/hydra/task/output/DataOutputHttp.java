/*
 * Copyright 2014 AddThis.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.task.output;


import com.addthis.codec.annotations.FieldConfig;

/**
 * This output sink <span class="hydra-summary">sends the output stream to a http sink</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>output:{
 *   type : "http",
 *   path : ["http://addthis.com/helo/world"],
 *   writer : {
 *     fields : ["foo", "baz", "baz],
 *     format : {
 *         type: "channel",
 *     }
 *   },
 * }</pre>
 *
 * @user-reference
 */
public class DataOutputHttp extends AbstractDataOutput {

    /**
     * Output configuration parameters. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private HttpOutputWriter writer;


    public DataOutputHttp setWriter(HttpOutputWriter writer) {
        this.writer = writer;
        return this;
    }

    @Override protected AbstractOutputWriter getWriter() {
        return writer;
    }
}

