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
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

/**
 * simple settings wrapper allows changes to Spawn
 */
public class Settings {

    private final Spawn spawn;

    public Settings(Spawn spawn) {
        this.spawn = spawn;
    }

    public String getDebug() {
        return spawn.debug;
    }

    public void setDebug(String debug) {
        spawn.debug = debug;
    }

    public int getDefaultReplicaCount() {
        return Spawn.DEFAULT_REPLICA_COUNT;
    }

    public String getQueryHost() {
        return spawn.queryHost;
    }

    public String getSpawnHost() {
        return spawn.spawnHost;
    }

    public void setQueryHost(String queryHost) {
        spawn.queryHost = queryHost;
    }

    public void setSpawnHost(String spawnHost) {
        spawn.spawnHost = spawnHost;
    }

    public boolean getQuiesced() {
        return spawn.getQuiesced();
    }

    public void setQuiesced(boolean quiesced) {
        Spawn.quiesceCount.clear();
        if (quiesced) {
            Spawn.quiesceCount.inc();
        }
        spawn.spawnState.quiesce.set(quiesced);
        spawn.writeState();
    }

    public String getDisabled() {
        return Strings.join(spawn.spawnState.disabledHosts.toArray(), ",");
    }

    public void setDisabled(String disabled) {
        List<String> newDisabledHosts = Arrays.asList(disabled.split(","));
        spawn.spawnState.disabledHosts.addAll(newDisabledHosts);
        spawn.spawnState.disabledHosts.retainAll(newDisabledHosts);
        spawn.writeState();
    }

    public JSONObject toJSON() throws JSONException {
        return new JSONObject().put("debug", spawn.debug)
                               .put("quiesce", spawn.spawnState.quiesce.get())
                               .put("queryHost", spawn.queryHost)
                               .put("spawnHost", spawn.spawnHost)
                               .put("disabled", getDisabled())
                               .put("defaultReplicaCount", Spawn.DEFAULT_REPLICA_COUNT);
    }
}
