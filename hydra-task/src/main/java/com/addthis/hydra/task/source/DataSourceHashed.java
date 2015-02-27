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

import javax.annotation.Nonnull;

import java.util.NoSuchElementException;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.common.hash.PluggableHashFunction;
import com.addthis.hydra.task.run.TaskRunConfig;

import com.google.common.collect.ImmutableList;

/**
 * This data source <span class="hydra-summary">shards the input source by hashing on a bundle field</span>.
 *
 * @user-reference
 * @hydra-name hashed
 */
public class DataSourceHashed extends TaskDataSource {

    /**
     * Underlying data source from which data is fetched. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataSource stream;

    /**
     * Name of the bundle field whose values are used as input to a hash function. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String hashKey;

    /**
     * Total number of shards. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int shardTotal;

    @FieldConfig
    private TaskRunConfig config;

    private Bundle peek;

    private Integer[] shards;

    @Override
    public void init() {
        shards = config.calcShardList(shardTotal);
        stream.init();
    }

    @Nonnull @Override public ImmutableList<String> outputRootDirs() {
        return stream.outputRootDirs();
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public Bundle peek() {
        Bundle tmp;
        while (peek == null && (tmp = stream.peek()) != null) {
            int hash = Math.abs(PluggableHashFunction.hash(tmp.getValue(tmp.getFormat().getField(hashKey)).asString().toString()) % shardTotal);
            for (Integer shard : shards) {
                if (hash == shard) {
                    return tmp;
                }

            }
            stream.next();
        }
        return null;
    }

    @Override
    public Bundle next() {
        Bundle ret;
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
