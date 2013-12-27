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
package com.addthis.hydra.data.query.op;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.AbstractRowOp;


/**
 * <p>This query operation <span class="hydra-summary">skips N rows</span>.
 * <p/>
 * <p>Examples:</p>
 * <pre>
 *     skip=100 // skip the first one hundred rows
 * </pre>
 *
 * @user-reference
 * @hydra-name skip
 */
public class OpSkip extends AbstractRowOp {

    private int skip[];

    private boolean on;
    private int rem;

    public OpSkip(String args) {
        skip = csvToInts(args);
        rem = skip[0] + 1;
        on = false;
    }

    @Override
    public Bundle rowOp(Bundle line) {
        if (--rem == 0) {
            if (skip.length > 2) {
                rem = on ? skip[2] : skip[1];
            } else if (skip.length > 1) {
                rem = skip[1];
            }
            on = !on;
        }
        return on ? line : null;
    }
}
