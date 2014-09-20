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

import java.util.Arrays;

import com.addthis.basis.util.Strings;

import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

/**
 * simple settings wrapper allows changes to Spawn
 */
public class Settings {

    private Spawn spawn;

    public Settings(Spawn spawn) {this.spawn = spawn;}

    public String getDebug() {
        return spawn.debug;
    }

    public void setDebug(String debug) {
        spawn.debug = debug;
        spawn.writeState();
    }

    public int getDefaultReplicaCount() {
        return Spawn.DEFAULT_REPLICA_COUNT;
    }

    public String getQueryHost() {
        return spawn.queryHost;
    }

    public void setQueryHost(String queryHost) {
        spawn.queryHost = queryHost;
        spawn.writeState();
    }

    public boolean getQuiesced() {
        return spawn.quiesce;
    }

    public void setQuiesced(boolean quiesced) {
        Spawn.quiesceCount.clear();
        if (quiesced) {
            Spawn.quiesceCount.inc();
        }
        spawn.quiesce = quiesced;
        spawn.writeState();
    }

    public String getDisabled() {
        synchronized (spawn.disabledHosts) {
            return Strings.join(spawn.disabledHosts.toArray(), ",");
        }
    }

    public void setDisabled(String disabled) {
        synchronized (spawn.disabledHosts) {
            spawn.disabledHosts.clear();
            spawn.disabledHosts.addAll(Arrays.asList(disabled.split(",")));
        }
    }

    public JSONObject toJSON() throws JSONException {
        return new JSONObject().put("debug", spawn.debug).put("quiesce", spawn.quiesce)
                               .put("queryHost", spawn.queryHost)
                               .put("disabled", getDisabled())
                               .put("defaultReplicaCount", Spawn.DEFAULT_REPLICA_COUNT);
    }
}
