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

package com.addthis.hydra.data.query.source;

import java.util.Map;

import com.addthis.hydra.data.query.engine.QueryEngine;

public class LiveSearchRunner extends SearchRunner {

    private final QueryEngine engine;

    public LiveSearchRunner(Map<String, String> options, String dirString,
            DataChannelToInputStream bridge, QueryEngine engine) throws Exception {
        super(options, dirString, bridge);
        this.engine = engine;
    }

    @Override
    protected QueryEngine getEngine() throws Exception {
        return engine;
    }
}
