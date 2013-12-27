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
package com.addthis.hydra.task.map;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;
import com.addthis.hydra.common.plugins.PluginReader;

@Codec.Set(classMapFactory = StreamBuilder.CMAP.class)
public abstract class StreamBuilder {

    private static ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }
    };

    public static class CMAP implements ClassMapFactory {

        public ClassMap getClassMap() {
            return cmap;
        }
    }

    static {
        PluginReader.registerPlugin("-stream-builder.classmap", cmap, StreamBuilder.class);
    }

    public abstract void init();

    public abstract void process(Bundle row, StreamEmitter emitter);

    // todo: make abstract and force implementation
    public void streamComplete(StreamEmitter streamEmitter) {
    }
}
