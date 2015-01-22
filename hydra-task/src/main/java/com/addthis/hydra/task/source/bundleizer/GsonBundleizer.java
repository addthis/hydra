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
package com.addthis.hydra.task.source.bundleizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import com.google.common.annotations.Beta;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

/** Reads sequential objects from a large json array. */
@Beta
public class GsonBundleizer extends BundleizerFactory {

    @Override
    public void open() { }

    @Override
    public Bundleizer createBundleizer(final InputStream inputArg, final BundleFactory factoryArg) {
        return new Bundleizer() {
            private final BundleFactory factory = factoryArg;
            private final JsonParser parser = new JsonParser();
            private final JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(inputArg)));

            {
                try {
                    reader.beginArray();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Bundle next() throws IOException {
                if (reader.hasNext()) {
                    JsonObject nextElement = parser.parse(reader).getAsJsonObject();
                    Bundle next = factory.createBundle();
                    BundleFormat format = next.getFormat();
                    for (Map.Entry<String, JsonElement> entry : nextElement.entrySet()) {
                        next.setValue(format.getField(entry.getKey()), fromGson(entry.getValue()));
                    }
                    return next;
                } else {
                    return null;
                }
            }
        };
    }

    public ValueObject fromGson(JsonElement gson) {
        if (gson.isJsonNull()) {
            return null;
        } else if (gson.isJsonObject()) {
            JsonObject object = gson.getAsJsonObject();
            ValueMap map = ValueFactory.createMap();
            for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                map.put(entry.getKey(), fromGson(entry.getValue()));
            }
            return map;
        } else if (gson.isJsonArray()) {
            JsonArray gsonArray = gson.getAsJsonArray();
            ValueArray valueArray = ValueFactory.createArray(gsonArray.size());
            for (JsonElement arrayElement : gsonArray) {
                valueArray.add(fromGson(arrayElement));
            }
            return valueArray;
        } else {
            JsonPrimitive primitive = gson.getAsJsonPrimitive();
            if (primitive.isNumber()) {
                return ValueFactory.create(primitive.getAsDouble());
            } else {
                return ValueFactory.create(primitive.getAsString());
            }
        }
    }
}
