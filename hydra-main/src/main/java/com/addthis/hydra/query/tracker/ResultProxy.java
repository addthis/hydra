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

package com.addthis.hydra.query.tracker;

import java.util.List;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.query.source.QueryConsumer;

/**
 * proxy to watch for end of query to enable state cleanup
 */
class ResultProxy implements DataChannelOutput {

    final QueryEntry entry;
    final DataChannelOutput consumer;


    ResultProxy(QueryEntry entry, DataChannelOutput consumer) {
        this.entry = entry;
        this.consumer = new QueryConsumer(consumer);
    }

    @Override
    public void send(Bundle row) {
        consumer.send(row);
        entry.lines.incrementAndGet();
    }

    @Override
    public void send(List<Bundle> bundles) {
        if ((bundles != null) && !bundles.isEmpty()) {
            entry.lines.addAndGet(bundles.size());
            consumer.send(bundles);
        }
    }

    @Override
    public void sendComplete() {
        try {
            consumer.sendComplete();
            entry.finish(null);
        } catch (Exception ex) {
            sourceError(DataChannelError.promote(ex));
        }
    }

    @Override
    public void sourceError(DataChannelError ex) {
        entry.finish(ex);
        consumer.sourceError(ex);
    }

    @Override
    public Bundle createBundle() {
        return consumer.createBundle();
    }
}
