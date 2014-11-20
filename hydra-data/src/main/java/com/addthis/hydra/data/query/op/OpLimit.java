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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * This query operation <span class="hydra-summary">limits the number of rows that are generated</span>.
 * <p/>
 * There are two forms of this operation. "sendCount=N" limits the number of rows that
 * are emitted to N rows. "sendCount=M:N" skips over the first M rows and then limits the
 * number of rows that are emitted to N rows.
 *
 * @user-reference
 * @hydra-name limit
 */
public class OpLimit extends AbstractQueryOp {

    private static final Logger log = LoggerFactory.getLogger(OpLimit.class);

    /** used to ensure sendComplete is only called once; not needed in theory, but unclear */
    private boolean done;
    private int     sendCount;
    private int     skipCount;

    private final int originalSkipCount;

    public OpLimit(int sendCount, int skipCount, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        this.originalSkipCount = sendCount;
        this.skipCount = skipCount;
        this.sendCount = sendCount;
        if (originalSkipCount <= 0) {
            throw new IllegalArgumentException("sendCount must be > 0");
        }
    }

    public OpLimit(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        String[] v = Strings.splitArray(args, ":");
        if (v.length == 1) {
            this.originalSkipCount = Integer.parseInt(v[0]);
            this.skipCount = 0;
        } else if (v.length == 2) {
            this.originalSkipCount = Integer.parseInt(v[1]);
            this.skipCount = Integer.parseInt(v[0]);
        } else {
            throw new IllegalArgumentException("OpLimit requires [1,2] integer parameters");
        }
        this.sendCount = originalSkipCount;
        if (originalSkipCount <= 0) {
            throw new IllegalArgumentException("sendCount must be > 0");
        }
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        // skipCount bundles until skipCount is reached
        if (skipCount > 0) {
            skipCount--;
            return;
        }
        // emit bundles until sendCount is reached
        if (sendCount > 0) {
            sendCount--;
            getNext().send(row);
            // if we just emitted the last bundle, complete the operation
            if (sendCount == 0) {
                sendComplete();
                opPromise.trySuccess(); // best-effort signal to parent op-processor and source
                log.debug("OpLimit: sendCount reached {} and sendComplete has been called",
                          originalSkipCount);
            }
        } else {
            // TODO: signal to sender so they don't have to pro-actively check the promise
            log.trace("received bundle after sendCount reached; possibly expected to some extent");
        }
    }

    @Override
    public void sendComplete() {
        if (!done) {
            done = true;
            getNext().sendComplete();
        }
    }
}
