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
package com.addthis.hydra.job.web.entities;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONObject;

public class TopicEvent implements Codable {

    @FieldConfig(codable = true)
    String topic;
    @FieldConfig(codable = true)
    Object message;
    @FieldConfig(codable = true)
    long time;

    public TopicEvent(String topic, Object message) {
        this.topic = topic;
        this.message = message;
        this.time = System.currentTimeMillis();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Object getMessage() {
        return message;
    }

    public void setMessage(Object message) {
        this.message = message;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public JSONObject toJSON() {
        try {
            return CodecJSON.encodeJSON(this);
        } catch (Exception e) {
            return new JSONObject();
        }
    }
}
