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

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;

@Codec.Set(classMapFactory = ValuesOutput.CMAP.class)
public abstract class ValuesOutput implements Codec.Codable, DataChannelOutput {

    private static ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }
    };

    /**
     * handles serialization maps
     */
    public static class CMAP implements ClassMapFactory {

        public ClassMap getClassMap() {
            return cmap;
        }
    }

    public static void registerClass(String name, Class<? extends ValuesOutput> clazz) {
        cmap.add(name, clazz);
    }

    /** setup default serialization types */
    static {
        registerClass("file", ValuesOutputFile.class);
    }

    public abstract void open() throws Exception;
}
