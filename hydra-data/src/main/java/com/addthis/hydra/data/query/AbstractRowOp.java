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
package com.addthis.hydra.data.query;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;

import io.netty.channel.ChannelProgressivePromise;

/**
 * this class of operator should modify, drop or create lines
 * using the existing rows as sources.
 */
public abstract class AbstractRowOp extends AbstractQueryOp {

    public AbstractRowOp(ChannelProgressivePromise queryPromise) {
        super(queryPromise);
    }

    public abstract Bundle rowOp(Bundle row);

    @Override
    public void send(Bundle row) throws DataChannelError {
        Bundle rl = rowOp(row);
        if (rl != null) {
            getNext().send(rl);
        }
    }

    @Override
    public void sendComplete() {
        getNext().sendComplete();
    }
}
