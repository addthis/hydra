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
package com.addthis.hydra.job.web.websocket;

import javax.websocket.Session;

import java.io.IOException;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.spawn.ClientEvent;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a WebSocket {@link Session} object and keeps track of update events going out to the client.
 */
public class SpawnWebSocketSession {

    private static final Logger log = LoggerFactory.getLogger(SpawnWebSocketSession.class);

    /** Max number of events to be queued */
    private static final int clientDropQueueSize = Parameter.intValue("spawn.client.drop.queue", 2000);

    private static final Counter nonConsumingClientDropCounter = Metrics.newCounter(Spawn.class, "clientDropsSpawnV2");

    /** The underlying WebSocket {@link Session} object */
    private Session session;

    /** A queue of events to queue up and push to websockets at intervals */
    private final LinkedBlockingQueue<ClientEvent> eventQueue;

    public SpawnWebSocketSession(Session session) {
        this.session = session;
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public Session getSession() {
        return session;
    }

    /**
     * Adds an update event to the queue that will be drained periodically.
     *
     * If queue is at max size, the client may be dead. In that case this method will clear the queue.
     *
     * @return {@code true} if the event is queued successfully. {@code false} if queue is at max size.
     */
    public boolean addEvent(ClientEvent event) {
        if (eventQueue.size() <= clientDropQueueSize) {
            eventQueue.add(event);
            return true;
        } else {
            // Queue has grown too big. Client does not appear to be consuming. Drop the socket to prevent an ugly OOM.
            eventQueue.clear();
            nonConsumingClientDropCounter.inc();
            return false;
        }
    }

    /**
     * Sends queued update events to the WebSocket client.
     *
     * @param maxNumber Max number of events to send.
     * @return Number of events actually sent
     */
    public int drainEvents(int maxNumber) {
        int events = 0;
        try {
            JSONArray eventArray = new JSONArray();
        for (int i = 0; i < eventQueue.size() && i < maxNumber; i++) {
            ClientEvent event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
            eventArray.put(event.toJSON());
            events++;
        }
        if (events > 0) {
            String eventArrayString = eventArray.toString();
            sendMessageByTopic("event.batch.update", eventArrayString);
        }} catch (Exception ex) {
            log.warn("Error sending batch updates to WebSocket: {}", ex.toString());
        }
        return events;
    }

    /**
     * This method is used to send json message to web socket by topic
     *
     * @param topic   the topic of the message
     * @param message the actual message payload in text format
     */
    private void sendMessageByTopic(String topic, String message) throws JSONException, IOException {
        JSONObject json = new JSONObject();
        json.put("topic", topic);
        json.put("message", message);
        session.getBasicRemote().sendText(json.toString());
    }

}
