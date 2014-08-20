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
package com.addthis.hydra.job.web.jersey;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Optional;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.yammer.dropwizard.auth.AuthenticationException;
import com.yammer.dropwizard.auth.Authenticator;
import com.yammer.dropwizard.auth.basic.BasicCredentials;

class SpawnAuthInjectable<T> extends AbstractHttpContextInjectable<T> {

    private static final String USER_HEADER_NAME = "Username";

    private final Authenticator<BasicCredentials, T> authenticator;
    private final String realm;
    private final boolean required;

    SpawnAuthInjectable(Authenticator<BasicCredentials, T> authenticator, String realm, boolean required) {
        this.authenticator = authenticator;
        this.realm = realm;
        this.required = required;
    }

    public Authenticator<BasicCredentials, T> getAuthenticator() {
        return authenticator;
    }

    public String getRealm() {
        return realm;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public T getValue(HttpContext c) {
        final String username = c.getRequest().getHeaderValue(USER_HEADER_NAME);
        try {
            if (username != null) {
                final BasicCredentials credentials = new BasicCredentials(username, "");
                final Optional<T> result = authenticator.authenticate(credentials);
                if (result.isPresent()) {
                    return result.get();
                }
            }
        } catch (AuthenticationException e) {
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }

        if (required) {
            throw new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED)
                    .entity("Credentials are required to access this resource.")
                    .type(MediaType.TEXT_PLAIN_TYPE)
                    .build());
        }
        return null;
    }
}
