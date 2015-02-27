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
package com.addthis.hydra.task.output;


import javax.annotation.Nonnull;

import java.util.List;

import com.addthis.codec.annotations.FieldConfig;

import com.google.common.collect.ImmutableList;

/**
 * This output sink <span class="hydra-summary">shards the output stream to one or more files</span>.
 * <p/>
 * <p>The elements of the 'path' parameter can be special substitution variables.
 * In the following table "N" and "FIELD_NAME" are user-specified values.
 * The other characters must appear exactly as shown here.
 * <table>
 * <tr>
 * <th>Variable</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>[[job]]</td>
 * <td>job number</td>
 * </tr>
 * <tr>
 * <td>[[nodes]]</td>
 * <td>total number of nodes</td>
 * </tr>
 * <tr>
 * <td>[[node]]</td>
 * <td>node number</td>
 * </tr>
 * <tr>
 * <td>[[nodes:N]]</td>
 * <td>total number of nodes written with N digits</td>
 * </tr>
 * <tr>
 * <td>[[node:N]]</td>
 * <td>node number written with N digits</td>
 * </tr>
 * <tr>
 * <td>{{FIELD_NAME}}</td>
 * <td>value of this field from the bundle</td>
 * </tr>
 * </table>
 * <p>Example:</p>
 * <pre>output:{
 *   type : "file",
 *   path : ["{{DATE_YMD}}", "/", "{{SHARD}}"],
 *   writer : {
 *     maxOpen : 1024,
 *     flags : {
 *       maxSize : "64M",
 *       compress : true,
 *     },
 *     factory : {
 *       dir : "split",
 *       multiplex : true,
 *     },
 *     format : {
 *       type : "channel",
 *     },
 *   },
 * }</pre>
 *
 * @user-reference
 * @hydra-name file
 */
public class DataOutputFile extends AbstractDataOutput {

    /**
      * Output configuration parameters. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private OutputWriter writer;


    public DataOutputFile setWriter(OutputWriter writer) {
        this.writer = writer;
        return this;
    }

    @Override
    AbstractOutputWriter getWriter() {
        return writer;
    }

    @Override @Nonnull
    public ImmutableList<String> outputRootDirs() {
        return writer.outputRootDirs();
    }
}
