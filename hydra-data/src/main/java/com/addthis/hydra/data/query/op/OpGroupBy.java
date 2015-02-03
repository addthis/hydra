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

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import com.addthis.basis.util.MemoryCounter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.hydra.data.query.AbstractQueryOp;
import com.addthis.hydra.data.query.QueryMemTracker;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.op.merge.MergeConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/**
 * <p>This query operation <span class="hydra-summary">groups rows together</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * 0 A 3
 * 1 A 1
 * 1 B 2
 * 0 A 5
 *
 * groupby=k:limit=1
 *
 * A 3
 * B 2
 * </pre>
 *
 * @user-reference
 * @hydra-name groupby
 */
public class OpGroupBy extends AbstractQueryOp {
    private static final Logger log = LoggerFactory.getLogger(OpGroupBy.class);

    private final Map<String, QueryOp> resultTable = new HashMap<>();
    private final ListBundleFormat format = new ListBundleFormat();

    private final long memTip;
    private final long rowTip;
    private long memTotal;

    private final String queryDeclaration;

    private final MergeConfig mergeConfig;
    @MemoryCounter.Mem(estimate = false)
    private final QueryOpProcessor processor;
    private final OpForward forwardingOp;
    @MemoryCounter.Mem(estimate = false)
    private final ChannelFutureListener errorForwarder;

    public OpGroupBy(QueryOpProcessor processor, String args, ChannelProgressivePromise opPromise) {
        super(opPromise);
        this.memTotal = 0;
        this.processor = processor;
        this.memTip = processor.memTip();
        this.rowTip = processor.rowTip();
        if (!args.contains(":")) {
            throw new IllegalStateException("groupby query argument missing ':'");
        }
        String[] components = args.split(":", 2);
        this.mergeConfig = new MergeConfig(components[0]);
        this.queryDeclaration = components[1];
        this.forwardingOp = new OpForward(opPromise, getNext());
        // ops don't usually fail promises themselves, so this logic is unlikely to activate, but might as well
        this.errorForwarder = channelFuture -> {
            if (!channelFuture.isSuccess()) {
                opPromise.tryFailure(channelFuture.cause());
            }
        };
        opPromise.addListener(channelFuture -> {
            if (channelFuture.isSuccess()) {
                for (QueryOp queryOp : resultTable.values()) {
                    queryOp.getOpPromise().trySuccess();
                }
            } else {
                Throwable failureCause = channelFuture.cause();
                for (QueryOp queryOp : resultTable.values()) {
                    queryOp.getOpPromise().tryFailure(failureCause);
                }
            }
        });
    }

    @Override
    public void setNext(QueryMemTracker memTracker, QueryOp next) {
        super.setNext(memTracker, next);
        forwardingOp.setForwardingTarget(next);
    }

    /**
     * Generate new promise for the child operation.
     *
     * @param opPromise promise of the 'groupby' query operation
     *
     * @return generated promise
     */
    private ChannelProgressivePromise generateNewPromise(ChannelProgressivePromise opPromise) {
        final ChannelProgressivePromise result;
        if (opPromise.channel() == null) {
            result = new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE);
        } else {
            result = opPromise.channel().newProgressivePromise();
        }
        result.addListener(errorForwarder);
        return result;
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        if (opPromise.isDone()) {
            return;
        }
        String key = mergeConfig.handleBindAndGetKey(row, format);
        QueryOp queryOp = resultTable.computeIfAbsent(key, mapKey -> {
            ChannelProgressivePromise newPromise = generateNewPromise(opPromise);
            QueryOp newQueryOp = QueryOpProcessor.generateOps(processor, newPromise, forwardingOp, queryDeclaration);
            memTotal += MemoryCounter.estimateSize(newQueryOp);
            return newQueryOp;
        });
        memTotal -= MemoryCounter.estimateSize(queryOp);
        queryOp.send(row);
        memTotal += MemoryCounter.estimateSize(queryOp);

        // If we're not tipping to disk, and the tips are set, then we will issue errors if we pass them
        if ((memTip > 0) && (memTotal > memTip)) {
            throw new DataChannelError("Memory usage of gathered objects exceeds allowed " + memTip);
        }

        if ((rowTip > 0) && (resultTable.size() > rowTip)) {
            throw new DataChannelError("Number of gathered rows exceeds allowed " + rowTip);
        }
    }

    @Override
    public void sendComplete() {
        for (QueryOp queryOp : resultTable.values()) {
            if (opPromise.isDone()) {
                break;
            } else {
                if (!queryOp.getOpPromise().isDone()) {
                    queryOp.sendComplete();
                    queryOp.getOpPromise().trySuccess();
                }
            }
        }
        QueryOp next = getNext();
        next.sendComplete();
    }

    @Override
    public void close() throws IOException {
        for (QueryOp queryOp : resultTable.values()) {
            QueryOp currentOp = queryOp;
            while (currentOp != null) {
                try {
                    currentOp.close();
                } catch (Throwable ex) {
                    // hopefully an "out of off heap/direct memory" error if not an exception
                    log.error("unexpected exception or error while closing query op", ex);
                }
                currentOp = currentOp.getNext();
            }
        }
    }
}
