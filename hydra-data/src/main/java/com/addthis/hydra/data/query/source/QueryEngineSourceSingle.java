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

import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.query.source.QueryEngineSource;
import com.addthis.hydra.data.tree.DataTree;


public class QueryEngineSourceSingle extends QueryEngineSource {

    private final DataTree tree;

    public QueryEngineSourceSingle(DataTree tree) {
        this.tree = tree;
    }

    @Override
    public QueryEngine getEngineLease() {
        try {
            return new QueryEngine(tree);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
