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
package com.addthis.hydra.job.spawn;

import com.addthis.codec.codables.Codable;
import com.addthis.maljson.JSONObject;

/**
 * event queued to a browser ClientListener
 */
public final class ClientEvent implements Codable {

    private String topic;
    private JSONObject message;

    public ClientEvent(String topic, JSONObject message) {
        this.topic = topic;
        this.message = message;
    }

    public String topic() {
        return topic;
    }

    public JSONObject message() {
        return message;
    }

    public JSONObject toJSON() throws Exception {
        return new JSONObject().put("topic", topic).put("message", message);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ClientEvent) {
            ClientEvent ce = (ClientEvent) o;
            return ce.topic == topic && ce.message == message;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return message.hashCode();
    }
}
