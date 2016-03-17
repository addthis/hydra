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

import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import java.util.Arrays;

public class SearchContext {
    private final int startLine;
    private final String[] lines;

    public SearchContext(int matchedLine, String[] allLines, int buffer) {
        this.startLine = Math.max(0, matchedLine - buffer);
        int endLine = Math.min(allLines.length, matchedLine + buffer);
        this.lines = Arrays.copyOfRange(allLines, startLine, endLine);
    }

    public JSONArray toJSONArray() throws JSONException {
        JSONArray json = new JSONArray();
        for (int i = 0; i < lines.length; i++) {
            String text = lines[i];
            int lineNum = i + startLine + 1; // convert from 0 indexed lines to 1 indexed

            JSONObject jsonLine = new JSONObject();
            jsonLine.put("line", lineNum);
            jsonLine.put("text", text);
            json.put(jsonLine);
        }

        return json;
    }
}
