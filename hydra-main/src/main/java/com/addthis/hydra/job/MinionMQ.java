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
package com.addthis.hydra.job;

import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.mq.MessageListener;

/**
 * Messaging interface
 */
public interface MinionMQ {
    /**
     * connect to messaging infrastructure
     *
     * @param hostUUID - the host being connected
     * @throws Exception - thrown if there is an error connecting to MQ
     */
    void connect(String hostUUID, MessageListener listener) throws Exception;

    /**
     * Send a message to the control channel
     *
     * @param msg - the message to send
     */
    boolean sendControlMessage(HostMessage msg) ;

    /**
     * Send a message to the job channel
     *
     * @param msg - the message to send
     */
    boolean sendStatusMessage(HostMessage msg) ;

    /**
     *
     * @return next available host message
     */
    CommandTaskKick pollKickMessages() throws InterruptedException ;

    /**
     * closes all of the connections
     */
    void disconnect() ;
}
