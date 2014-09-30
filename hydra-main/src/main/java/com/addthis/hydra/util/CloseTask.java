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
package com.addthis.hydra.util;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CloseTask.class);
    private final AutoCloseable closeable;

    public CloseTask(AutoCloseable closeable) {
        this.closeable = closeable;
    }

    @Override public void run() {
        try {
            closeable.close();
        } catch (Throwable e) {
            log.error("Propagating error encountered while closing {}", closeable, e);
            Throwables.propagate(e);
        }
    }
}
