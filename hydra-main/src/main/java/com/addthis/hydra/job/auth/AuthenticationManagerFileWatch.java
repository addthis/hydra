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
package com.addthis.hydra.job.auth;

import javax.annotation.Nonnull;
import javax.annotation.Syntax;

import java.io.IOException;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around the static authentication manager
 * that watches a file for changes and loads the latest
 * static manager configuration into memory.
 */
class AuthenticationManagerFileWatch extends AuthenticationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationManagerFileWatch.class);

    @Nonnull
    private volatile AuthenticationManagerStatic manager;

    @Nonnull
    private final WatchService watcher;

    @Nonnull
    private final Path path;

    @JsonCreator
    public AuthenticationManagerFileWatch(@JsonProperty("path") Path path) throws IOException {
        @Syntax("HOCON") String content = new String(Files.readAllBytes(path));
        this.watcher = FileSystems.getDefault().newWatchService();
        this.manager = Configs.decodeObject(AuthenticationManagerStatic.class, content);
        this.path = path;
        path.getParent().register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
    }

    private void updateAuthenticationManager() {
        try {
            WatchKey watchKey = watcher.poll();
            boolean update = false;
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                // on overflow go ahead and update the manager
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    update = true;
                    break;
                }
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filepath = ev.context();
                if (path.equals(filepath)) {
                    update = true;
                    break;
                }
            }
            if (update) {
                @Syntax("HOCON") String content = new String(Files.readAllBytes(path));
                manager = Configs.decodeObject(AuthenticationManagerStatic.class, content);
            }
        } catch (IOException ex) {
            log.warn("IOException during file watch authentication manger: ", ex);
        }
    }

    @Override String login(String username, String password, boolean ssl) {
        updateAuthenticationManager();
        return manager.login(username, password, ssl);
    }

    @Override public boolean verify(String username, String password, boolean ssl) {
        updateAuthenticationManager();
        return manager.verify(username, password, ssl);
    }

    @Override User authenticate(String username, String secret) {
        updateAuthenticationManager();
        return manager.authenticate(username, secret);
    }

    @Override protected User getUser(String username) {
        updateAuthenticationManager();
        return manager.getUser(username);
    }

    @Override void logout(User user) {
        updateAuthenticationManager();
        manager.logout(user);
    }

    @Override ImmutableList<String> adminGroups() {
        updateAuthenticationManager();
        return manager.adminGroups();
    }

    @Override ImmutableList<String> adminUsers() {
        updateAuthenticationManager();
        return manager.adminUsers();
    }
}
