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
package com.addthis.hydra.job.web.jersey;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class User implements Codable {

    @FieldConfig(codable = true)
    private final String username;
    private final String dn;

    @FieldConfig(codable = true)
    private final boolean isAdmin;

    public User(String username, String dn) {
        this.username = username;
        this.isAdmin = false;
        this.dn = dn;
    }

    public User(String username, String dn, boolean isAdmin) {
        this.username = username;
        this.isAdmin = isAdmin;
        this.dn = dn;
    }

    public String getUsername() {
        return this.username;
    }

    public String getDN() {
        return this.dn;
    }

    public boolean getAdmin() {
        return this.isAdmin;
    }

    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }
}
