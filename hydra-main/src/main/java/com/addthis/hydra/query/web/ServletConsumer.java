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

package com.addthis.hydra.query.web;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * parent of all streaming response classes
 */
abstract class ServletConsumer implements DataChannelOutput {

    private static final Logger log = LoggerFactory.getLogger(ServletConsumer.class);

    HttpServletResponse response;
    PrintWriter writer;
    final Semaphore gate = new Semaphore(1);
    final AtomicBoolean done = new AtomicBoolean(false);
    final ListBundleFormat format = new ListBundleFormat();
    final long startTime;
    boolean error = false;

    ServletConsumer(HttpServletResponse response) throws IOException, InterruptedException {
        this.response = response;
        this.writer = response.getWriter();
        startTime = System.currentTimeMillis();
        gate.acquire();
    }

    void setDone() {
        if (done.compareAndSet(false, true)) {
            gate.release();
        }
    }

    void waitDone() throws InterruptedException, IOException {
        waitDone(QueryServlet.maxQueryTime);
    }

    void waitDone(final int waitInSeconds) throws InterruptedException, IOException {
        if (!done.get()) {
            try {
                gate.acquire();
            } finally {
                setDone();
            }
        }
    }

    @Override
    public void sourceError(DataChannelError ex) {
        try {
            response.getWriter().write(ex.getMessage());
            response.setStatus(500);
            error = true;
            log.error("", ex);
        } catch (IOException e) {
            log.warn("", "Exception sending error: " + e);
        } finally {
            setDone();
        }
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(format);
    }

    protected boolean isError() {
        return error;
    }
}
