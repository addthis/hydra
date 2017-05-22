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
package com.addthis.hydra.job.web.resources;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;

import com.addthis.hydra.job.auth.User;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.SpawnServiceConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/authentication")
public class AuthenticationResource {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationResource.class);

    private final Spawn spawn;

    private final SpawnServiceConfiguration configuration;

    public AuthenticationResource(Spawn spawn) {
        this.spawn = spawn;
        this.configuration = SpawnServiceConfiguration.SINGLETON;
    }

    @POST
    @Path("/login")
    @Produces(MediaType.TEXT_PLAIN)
    public Response login(@FormParam("user") String username,
                          @FormParam("password") String password,
                          @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == configuration.webPortSSL);
        try {
            String token = spawn.getPermissionsManager().login(username, password, usingSSL);
            Response.ResponseBuilder builder;
            if (token == null) {
                builder = Response.status(Response.Status.UNAUTHORIZED);
                builder.entity("Invalid credentials provided");
            } else {
                builder = Response.ok(token);
            }
            builder.header("Access-Control-Allow-Origin",
                            "http://" + uriInfo.getAbsolutePath().getHost() +
                            ":" + configuration.webPort);
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            log.warn("Internal error in authentication attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity("internal error").build();
        }
    }

    @POST
    @Path("/sudo")
    @Produces(MediaType.TEXT_PLAIN)
    public Response sudo(@FormParam("user") String username,
                         @FormParam("token") String token,
                         @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == configuration.webPortSSL);

        try {
            String sudoToken = spawn.getPermissionsManager().sudo(username, token, usingSSL);
            Response.ResponseBuilder builder;
            if (sudoToken == null) {
                builder = Response.status(Response.Status.UNAUTHORIZED);
                builder.entity("Invalid credentials provided or you don't have sudo privilege");
            } else {
                builder = Response.ok(sudoToken);
            }
            builder.header("Access-Control-Allow-Origin",
                           "http://" + uriInfo.getAbsolutePath().getHost() +
                           ":" + configuration.webPort);
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            log.warn("Internal error in sudo attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @POST
    @Path("/validate")
    @Produces(MediaType.TEXT_PLAIN)
    public Response validate(@FormParam("user") String username,
                             @FormParam("token") String token,
                             @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == configuration.webPortSSL);

        try {
            User user = spawn.getPermissionsManager().authenticate(username, token);
            Response.ResponseBuilder builder = Response.ok(Boolean.toString(user != null));
            builder.header("Access-Control-Allow-Origin",
                           "http://" + uriInfo.getAbsolutePath().getHost() +
                           ":" + configuration.webPort);
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            // TODO: don't show "Accept out https certificate" and hydra.png
            log.warn("Internal error in validation attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity("internal error").build();
        }
    }

    @POST
    @Path("/evict")
    public Response evict(@FormParam("user") String user,
                      @FormParam("token") String token,
                      @FormParam("sudo") String sudo,
                      @FormParam("target") String target) {
        Response.ResponseBuilder builder;
        if (target == null) {
            builder = Response.status(Response.Status.BAD_REQUEST);
            builder.entity("target argument missing");
        } else if ((target.equals(user) && (spawn.getPermissionsManager().authenticate(user, token) == null)) ||
                   (!target.equals(user) && !spawn.getPermissionsManager().adminAction(user, token, sudo))) {
            builder = Response.status(Response.Status.UNAUTHORIZED);
            builder.entity("Invalid credentials provided");
        } else if (!spawn.getPermissionsManager().evict(target)) {
            builder = Response.ok("User " + target + " was not logged in");
        } else {
            builder = Response.ok("User " + target + " evicted");
        }
        return builder.build();
    }

    @POST
    @Path("/isadmin")
    @Produces(MediaType.TEXT_PLAIN)
    public Response isAdmin(@FormParam("user") String username,
                            @FormParam("token") String token,
                            @Context UriInfo uriInfo) {
        User user = spawn.getPermissionsManager().authenticate(username, token);
        boolean isamdin = spawn.getPermissionsManager().isamdin(user);
        Response.ResponseBuilder builder;
        if (!isamdin) {
            builder = Response.ok(Boolean.toString(false));
        } else {
            builder = Response.ok(Boolean.toString(true));
        }
        builder.header("Access-Control-Allow-Origin",
                       "http://" + uriInfo.getAbsolutePath().getHost() +
                       ":" + configuration.webPort);
        builder.header("Access-Control-Allow-Methods", "POST");
        return builder.build();
    }

    @POST
    @Path("/unsudo")
    @Produces(MediaType.TEXT_PLAIN)
    public Response unsudo(@FormParam("username") String username,
                           @FormParam("token") String token,
                           @FormParam("sudo") String sudo) {
        Response.ResponseBuilder builder;
        if (username == null) {
            builder = Response.status(Response.Status.BAD_REQUEST);
            builder.entity("username argument missing");
        } else if (!spawn.getPermissionsManager().unsudo(username)) {
            builder = Response.ok("User " + username + " was not logged in");
        } else {
            builder = Response.ok("User " + username + " is degraded to an ordinary user");
        }
        return builder.build();
    }

    @POST
    @Path("/logout")
    public void logout(@FormParam("user") String username,
                       @FormParam("token") String token,
                       @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == configuration.webPortSSL);
        try {
            spawn.getPermissionsManager().logout(username, token);
        } catch (Exception ex)  {
            log.warn("Internal error in logout attempt for user {} with ssl {}", username, usingSSL, ex);
        }
    }
}
