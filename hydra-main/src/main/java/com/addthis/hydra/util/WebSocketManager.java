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

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.io.StringWriter;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.hydra.job.Spawn;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * This class is a jetty request handler that opens web sockets with clients upon request, and keeps a set of open web sockets
 * so as to give a server the ability to notify all clients and send them messages.
 */
public class WebSocketManager extends WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(WebSocketManager.class);

    /**
     * A threadsafe list of open WebSockets
     */
    private final Set<MQWebSocket> webSockets = new CopyOnWriteArraySet<MQWebSocket>();

    /**
     * A json factory for any json serializing
     */
    private static final JsonFactory factory = new JsonFactory(new com.fasterxml.jackson.databind.ObjectMapper());

    /**
     * Object used as monitor that the WebUpdateThread waits on when there are no websockets, and then gets notified when a websocket arrives
     */
    private final Object monitor = new Object();
    /**
     * A flag used to shutdown the webupdate thread
     */
    private volatile boolean shutdown = false;
    /**
     * The update thread that pushes events to the client web sockets and waits until new websockets are created when there are none
     */
    private final Thread updateThread;

    public WebSocketManager() {
        this.updateThread = new Thread("WebUpdateThread") {
            @Override
            public void run() {
                try {
                    while (!shutdown) {
                        synchronized (monitor) {
                            if (webSockets.size() == 0) {
                                monitor.wait();
                            }
                        }
                        for (MQWebSocket webSocket : webSockets) {
                            webSocket.drainEvents(100);
                        }
                        Thread.sleep(200);
                    }
                } catch (Exception ex) {
                    log.warn("[WebSocketManager] Error updating websockets from in update thread.");
                    ex.printStackTrace();
                }
            }
        };
        this.updateThread.setDaemon(true);
        this.updateThread.start();
    }


    /**
     * This method will be called on every client connect. This method must
     * return a WebSocket-implementing-class to work on. WebSocket is just an
     * interface with different types of communication possibilities. You must
     * create and return a class which implements the necessary WebSocket
     * interfaces.
     */
    public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
        MQWebSocket webSocket = new MQWebSocket(request.getParameter("user"), request.getRemoteAddr());
        synchronized (monitor) {
            int numberOfSockets = webSockets.size();
            webSockets.add(webSocket);
            if (numberOfSockets == 0) {
                monitor.notify(); //to wake up update timer thread
            }
        }
        return webSocket;
    }

    /**
     * Creates and returns json representation of the websockets for UI reporting
     *
     * @return json array of websockets
     * @throws IOException
     */
    public String getWebSocketsJSON() throws IOException {
        StringWriter writer = new StringWriter();
        final JsonGenerator json = factory.createJsonGenerator(writer);
        json.writeStartArray();
        for (MQWebSocket webSocket : webSockets) {
            json.writeStartObject();
            json.writeStringField("username", webSocket.getUsername());
            json.writeNumberField("lastSeenTime", webSocket.getLastSeenTime());
            json.writeNumberField("loggedInTime", webSocket.getLoggedInTime());
            json.writeStringField("remoteAddress", webSocket.getRemoteAddress());
            json.writeEndObject();
        }
        json.writeEndArray();
        json.close();
        return writer.toString();
    }

    /**
     * Sends a string message to all connected web sockets.
     *
     * @param message
     */
    public void sendMessageToAll(String message) {
        for (MQWebSocket webSocket : webSockets) {
            webSocket.sendMessage(message);
        }
    }

    /**
     * Sends a JSON message to all websockets subscribed to a given topic
     *
     * @param topic
     * @param messageObject
     */
    public void sendMessageToAllByTopic(String topic, JSONObject messageObject) {
        String message = messageObject.toString();
        this.sendMessageToAllByTopic(topic, message);
    }

    /**
     * Sends a string message to all websockets subscribed to a given topic
     *
     * @param topic   the topic to use for the message
     * @param message the message payload in string format
     */
    public void sendMessageToAllByTopic(String topic, String message) {
        for (MQWebSocket webSocket : webSockets) {
            webSocket.sendMessageByTopic(topic, message);
        }
    }

    /**
     * Queue up an event for websockets
     *
     * @param event
     */
    public void addEvent(Spawn.ClientEvent event) {
        for (MQWebSocket webSocket : webSockets) {
            webSocket.addEvent(event);
        }
    }

    /**
     * Implements an anonymous class for the websocket with the necessary
     * WebSocket interfaces and the network logic.
     */
    private class MQWebSocket implements WebSocket.OnTextMessage {

        /**
         * The connection object of every WebSocket.
         */
        private Connection connection;

        /**
         * The time in millis when connection was established
         */
        private long loggedInTime;

        /**
         * The time in millis when last heartbeat was received
         */
        private long lastSeenTime;

        /**
         * The time in millis when this user was sent events
         */
        private long lastEventDrainTime;

        /**
         * Username client provided when connection was established
         */
        private String username;

        /**
         * Remote address from which client initiated connection
         */
        private String remoteAddress;

        /**
         * A queue of events to queue up and push to websockets at intervals
         */
        private final LinkedBlockingQueue<Spawn.ClientEvent> eventQueue = new LinkedBlockingQueue<Spawn.ClientEvent>();

        public MQWebSocket(String username, String remoteAddress) {
            this.username = username;
            this.remoteAddress = remoteAddress;
        }

        public MQWebSocket() {
            this("anonymous", "unkown");
        }

        /**
         * This method is defined by the WebSocket interface. It will be called
         * when a new WebSocket Connection is established.
         *
         * @param connection the newly opened connection
         */
        public void onOpen(Connection connection) {
            //System.out.println("[SERVER] Opened connection");
            connection.setMaxIdleTime(30000);
            // WebSocket has been opened. Store the opened connection
            this.connection = connection;
            // Add WebSocket in the global list of WebSocket instances
            long time = System.currentTimeMillis();
            this.loggedInTime = time;
            this.lastSeenTime = time;
            this.lastEventDrainTime = time;
        }

        /**
         * This method is defined by the WebSocket.OnTestMessage Interface. It
         * will be called when a new Text message has been received. In this
         * class, we will send the received message to all connected clients.
         *
         * @param data message sent by client to server in String format
         */
        public void onMessage(String data) {
            try {
                if (data.equals("ping")) {
                    this.connection.sendMessage("pong");
                } else {
                    this.connection.sendMessage("The server received your message: " + data);
                }
                this.lastSeenTime = System.currentTimeMillis();
            } catch (Exception ex) {
                log.warn("Error sending message to " + this.toString());
                ex.printStackTrace();
            }
        }

        /**
         * This method is defined by the WebSocket Interface. It will be called
         * when a WebSocket Connection is closed.
         *
         * @param closeCode the exit code of the connection in integer format
         * @param message   a human readable message for the exit code
         */
        public void onClose(int closeCode, String message) {
            webSockets.remove(this);
        }

        /**
         * This method is used to send a string message to websocket
         *
         * @param message text data to send to this particular websocket
         */
        public void sendMessage(String message) {
            try {
                //TODO: Does this send blocks?
                this.connection.sendMessage(message);
            } catch (IOException ex) {
                log.warn("Error sending message to web socket: " + message);
                ex.printStackTrace();
            }
        }

        /**
         * This method is used to send json message to web socket by topic
         *
         * @param topic   the topic of the message
         * @param message the actual message payload in text format
         */
        public void sendMessageByTopic(String topic, String message) {
            try {
                JSONObject json = new JSONObject();
                json.put("topic", topic);
                json.put("message", message);
                this.sendMessage(json.toString());
            } catch (Exception ex) {
                log.warn("Error encoding web socket message: " + message);
                ex.printStackTrace();
            }
        }

        public long getLoggedInTime() {
            return loggedInTime;
        }

        public void setLoggedInTime(long loggedInTime) {
            this.loggedInTime = loggedInTime;
        }

        public long getLastSeenTime() {
            return lastSeenTime;
        }

        public void setLastSeenTime(long lastSeenTime) {
            this.lastSeenTime = lastSeenTime;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getRemoteAddress() {
            return remoteAddress;
        }

        public void setRemoteAddress(String remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        public void addEvent(Spawn.ClientEvent event) {
            eventQueue.add(event);
        }

        public int drainEvents(int maxNumber) {
            int events = 0;
            try {
                JSONArray eventArray = new JSONArray();
                for (int i = 0; i < eventQueue.size() && i < maxNumber; i++) {
                    Spawn.ClientEvent event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    eventArray.put(event.toJSON());
                    events++;
                }
                if (events > 0) {
                    sendMessageByTopic("event.batch.update", eventArray.toString());
                }
            } catch (Exception ex) {
                log.warn("[WebSocketManager] error sending batch updates to web socket");
                ex.printStackTrace();
            }
            return events;
        }
    }
}
