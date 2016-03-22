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
package com.addthis.hydra.job.spawn.search;

import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

public class SearchResult {
    private final String id;
    private final int lineNum;
    private final int charStart;
    private final int charEnd;
    private final SearchContext searchContext;

    protected SearchResult(String id, int lineNum, int charStart, int charEnd, SearchContext searchContext) {
        this.id = id;
        this.lineNum = lineNum;
        this.charStart = charStart;
        this.charEnd = charEnd;
        this.searchContext = searchContext;
    }

    protected byte[] serialize() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("line", lineNum);
        json.put("start", charStart);
        json.put("end", charEnd);
        json.put("context", searchContext.toJSONArray());
        return json.toString().getBytes();
    }
}
