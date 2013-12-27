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
package com.addthis.hydra.mq;

import org.apache.zookeeper.Watcher;

import org.I0Itec.zkclient.IZkStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExpireListener implements IZkStateListener {

    private static final Logger log = LoggerFactory.getLogger(SessionExpireListener.class);
    private final ZkSessionExpirationHandler zkSessionExpirationHandler;

    public SessionExpireListener(ZkSessionExpirationHandler zkSessionExpirationHandler) {
        this.zkSessionExpirationHandler = zkSessionExpirationHandler;
    }

    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
    }

    public void handleNewSession() throws Exception {
        log.warn("Lost connection to ZK.  Handling new session");
        zkSessionExpirationHandler.handleExpiredSession();
    }

    // Does not handle at this time.
    public void handleSessionEstablishmentError(final Throwable error) throws Exception {
        // API forces wraped downgrade of throwable. Not cool
        throw new RuntimeException(error);
    }

}
