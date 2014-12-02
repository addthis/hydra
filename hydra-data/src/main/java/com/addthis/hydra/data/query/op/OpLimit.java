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

import java.util.regex.Pattern;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.op.merge.MergeConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;

/**
 * This query operation <span class="hydra-summary">limits the number of rows that are generated</span>.
 * <p/>
 * There are two forms of this operation. "limit=N" limits the number of rows that
 * are emitted to N rows. "limit=M:N" skips over the first M rows and then limits the
 * number of rows that are emitted to N rows. It is also possible to specifiy "limit=kkkkN"
 * or "limit=kkkkkN:M" where one or more "k" characters specify columns that are used
 * as keys. This limit will then be applied within the consecutive identical key columns.
 * One point to keep in mind is that limit=N is able to terminate early without processing
 * all results from a query but limit=kN is unable to terminate early.
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
    private String  lastkey = null;

    private final int initialSendCount;
    private final int initialSkipCount;
    private final MergeConfig mergeConfig;
    private final ListBundleFormat format = new ListBundleFormat();

    @VisibleForTesting
    public final static Pattern validInput = Pattern.compile("^k*\\d+(:\\d+)?");

    public OpLimit(int sendCount, int skipCount, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        Preconditions.checkArgument(sendCount > 0, "sendCount must be > 0");
        Preconditions.checkArgument(skipCount >= 0, "skipCount must be >= 0");
        this.initialSkipCount = skipCount;
        this.initialSendCount = sendCount;
        this.skipCount = skipCount;
        this.sendCount = sendCount;
        this.mergeConfig = null;
    }

    public OpLimit(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        Preconditions.checkArgument(validInput.matcher(args).matches(), "OpLimit arguments are not valid");

        if (args.startsWith("k")) {
            int pos = args.lastIndexOf('k');
            int numberKeys = pos + 1;
            mergeConfig = new MergeConfig(args.substring(0, numberKeys));
            args = args.substring(numberKeys);
        } else {
            mergeConfig = null;
        }

        String[] v = Strings.splitArray(args, ":");
        if (v.length == 1) {
            this.initialSkipCount = 0;
            this.initialSendCount = Integer.parseInt(v[0]);
        } else if (v.length == 2) {
            this.initialSkipCount = Integer.parseInt(v[0]);
            this.initialSendCount = Integer.parseInt(v[1]);
        } else {
            throw new IllegalArgumentException("OpLimit requires one or two integer parameters");
        }

        this.skipCount = initialSkipCount;
        this.sendCount = initialSendCount;

        Preconditions.checkArgument(sendCount > 0, "sendCount must be > 0");
        Preconditions.checkArgument(skipCount >= 0, "skipCount must be >= 0");
    }

    /**
     * If one or more key columns have been specified then
     * test the row for a change in the key columns.
     * Reset the counters if the key has changed.
     *
     * @param row current bundle that is being processed
     */
    private void testRowKeys(Bundle row) {
        if (mergeConfig != null) {
            String key = mergeConfig.handleBindAndGetKey(row, format);
            if (!key.equals(lastkey)) {
                skipCount = initialSkipCount;
                sendCount = initialSendCount;
                lastkey = key;
            }
        }
    }

    @Override
    public void send(Bundle row) throws DataChannelError {

        testRowKeys(row);

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
            // if mergeConfig is present then we must scan through all rows
            if (sendCount == 0 && mergeConfig == null) {
                sendComplete();
                opPromise.trySuccess(); // best-effort signal to parent op-processor and source
                log.debug("OpLimit: sendCount reached {} and sendComplete has been called",
                          initialSendCount);
            }
        } else if (mergeConfig == null) {
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
