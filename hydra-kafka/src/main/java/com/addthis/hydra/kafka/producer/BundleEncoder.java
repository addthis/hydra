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
package com.addthis.hydra.kafka.producer;

import java.io.IOException;

import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;

import org.apache.kafka.common.serialization.Serializer;

public class BundleEncoder implements Serializer<Bundle> {

    @Override
    public void configure(Map<String, ?> stringMap, boolean b) {
        // not needed
    }

    @Override
    public byte[] serialize(String topic, Bundle bundle) {
        if(bundle == null) {
            return null;
        }
        try {
            return DataChannelCodec.encodeBundle(bundle);
        } catch (IOException e) {
            //this exception is never actually thrown
            return null;
        }
    }

    @Override
    public void close() {
        // not needed
    }
}
