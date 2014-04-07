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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.QueryStatusObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This query operation <span class="hydra-summary">limits the number of rows that are generated</span>.
 * <p/>
 * <p>There are two forms of this operation. "limit=N" limits the number of rows that
 * are emitted to N rows. "limit=N:M" skips over the first N rows and then limits the
 * number of rows that are emitted to M rows.</p>
 *
 * @user-reference
 * @hydra-name limit
 */
public class OpLimit extends AbstractQueryOp {

    private int limit;
    private int originalLimit;
    private int offset;
    private boolean done;
    private static final Logger log = LoggerFactory.getLogger(OpLimit.class);

    private QueryStatusObserver queryStatusObserver = null;

    /**
     * @param limit
     */
    public OpLimit(int limit, int offset, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        setup(limit, offset);
    }

    public OpLimit(String args, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        String v[] = Strings.splitArray(args, ":");
        if (v.length == 1) {
            setup(Integer.parseInt(v[0]), 0);
        } else if (v.length > 1) {
            setup(Integer.parseInt(v[1]), Integer.parseInt(v[0]));
        }
    }

    protected void setup(int limit, int offset) {
        originalLimit = this.limit = limit;
        this.offset = offset;
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        if (queryStatusObserver.queryCompleted) {
            // Someone is attempting to send data even after we marked the query completed to true flag. This means
            // they are doing work and sending us bundles. Throw an exception because that needs to be checked.
            log.trace("Limit reached, sendComplete was called.");
            return;
        }

        if (offset > 0) {
            offset--;
            return;
        }
        if (limit > 0) {
            limit--;
            getNext().send(row);
        }
        if (limit == 0) {
            sendComplete();
            if (log.isDebugEnabled()) {
                log.debug("OpLimit: limit reached " + originalLimit + " and sendComplete has been called");
            }

            // Set the queryCompleted to true. After this point, no one should be calling OpLimit.send again, since
            // all sources should check the flag. If someone still calls send we will throw an exception at them. See
            // the top of this function
            queryStatusObserver.queryCompleted = true;
        }
    }

    @Override
    public void sendComplete() {
        if (!done) {
            getNext().sendComplete();
            done = true;
        }
    }
}
