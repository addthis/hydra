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
package com.addthis.hydra.data.channel;

import java.util.List;
import java.util.concurrent.Semaphore;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.hydra.data.query.QueryException;

/**
 * effective /dev/null for op chain that can block until complete send or error
 */
public class BlockingNullConsumer implements DataChannelOutput {

    private final ListBundleFormat format = new ListBundleFormat();
    private final Semaphore gate = new Semaphore(1);
    private QueryException exception;

    public BlockingNullConsumer() {
        try {
            gate.acquire();
        } catch (InterruptedException e) {
            throw new DataChannelError(e);
        }
    }

    public void waitComplete() throws Exception {
        gate.acquire();
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void sendComplete() {
        gate.release();
    }

    @Override
    public void send(Bundle row) {
    }

    @Override
    public void send(List<Bundle> bundles) {
    }

    @Override
    public void sourceError(Throwable ex) {
        exception = new QueryException(ex);
        sendComplete();
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }
}
