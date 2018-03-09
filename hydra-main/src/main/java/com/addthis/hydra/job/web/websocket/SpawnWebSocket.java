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

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import java.io.IOException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.addthis.hydra.job.spawn.ClientEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as both a WebSocket endpoint and a singleton for managing all Spawn WebSockets.
 * <p>
 * A new instance is created for each WebSocket connection. It's registered with Jetty via the Class object. For
 * simplicity's sake, static fields are used to track all instances instead of injection or a factory method.
 * <p>
 * Spawn pushes out update events to all websocket clients. Incoming messages are ignored.
 */
@ServerEndpoint("/ws")
public class SpawnWebSocket {

    private static final Logger log = LoggerFactory.getLogger(SpawnWebSocket.class);

    /** All active WebSocket sessions */
    private static final Set<SpawnWebSocketSession> sessions = new CopyOnWriteArraySet<>();

    /**
     * Object used as monitor that the WebUpdateThread waits on when there are no websockets, and then gets notified
     * when a websocket arrives
     */
    private static final Object monitor = new Object();

    static {
        createUpdateThread().start();
    }

    /**
     * Creates a new Thread that periodically push update events out to all active websocket sessions.
     */
    private static Thread createUpdateThread() {
        Thread thread = new Thread("WebSocketUpdateThread") {
            @Override
            public void run() {
                try {
                    while (true) {
                        synchronized (monitor) {
                            if (sessions.size() == 0) {
                                monitor.wait();
                            }
                        }
                        for (SpawnWebSocketSession session : sessions) {
                            session.drainEvents(100);
                        }
                        Thread.sleep(200);
                    }
                } catch (InterruptedException e) {
                    log.warn("[WebSocketManager] Error updating websockets from in update thread.");
                    e.printStackTrace();
                }
            }
        };
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Queue up an event for each active WebSocket session.
     */
    public static void addEvent(ClientEvent event) {
        Set<SpawnWebSocketSession> deadSessions = new HashSet<>();
        for (SpawnWebSocketSession session : sessions) {
            // session is dead if failed to add event
            if (!session.addEvent(event)) {
                deadSessions.add(session);
            }
        }
        // close and remove all dead sessions
        synchronized (monitor) {
            for (SpawnWebSocketSession session : deadSessions) {
                try {
                    session.getSession().close();
                } catch (Throwable e) {
                    // noop
                }
            }
            sessions.removeAll(deadSessions);
        }
    }

    @OnOpen
    public void onWebSocketConnect(Session session) {
        log.debug("onWebSocketConnect: {}", session);
        synchronized (monitor) {
            boolean firstSession = sessions.isEmpty();
            session.setMaxIdleTimeout(30000);
            sessions.add(new SpawnWebSocketSession(session));
            // wake up update thread now that we have an active websocket session
            if (firstSession) {
                monitor.notify();
            }
        }
    }

    /**
     * Simply echos client message unless it is "ping" in which case the return message will be "pong".
     */
    @OnMessage
    public void onWebSocketText(Session session, String message) {
        try {
            if ("ping".equals(message)) {
                session.getBasicRemote().sendText("pong");
            } else {
                session.getBasicRemote().sendText(message);
            }
        } catch (IOException e) {
            log.warn("Error responding to message: '{}': {}", message, e.toString());
        }
    }

    @OnClose
    public void onWebSocketClose(Session session, CloseReason reason) {
        log.debug("onWebSocketClose: {}, {}", session, reason);
        synchronized (monitor) {
            sessions.removeIf(e -> Objects.equals(e.getSession(), session));
        }
    }

}
