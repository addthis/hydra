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
package com.addthis.hydra.job.mq;

public class StatusTaskPort extends AbstractJobMessage {

    private static final long serialVersionUID = 8452018350634440050L;

    private int port;

    public StatusTaskPort(String host, String job, Integer node, int port) {
        super(host, job, node);
        this.port = port;
    }

    @Override
    public TYPE getMessageType() {
        return TYPE.STATUS_TASK_PORT;
    }

    public int getPort() {
        return port;
    }
}
