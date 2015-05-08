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
import com.addthis.hydra.job.web.SpawnService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/authentication")
public class AuthenticationResource {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationResource.class);

    private final Spawn spawn;

    public AuthenticationResource(Spawn spawn) {
        this.spawn = spawn;
    }

    @POST
    @Path("/login")
    @Produces(MediaType.TEXT_PLAIN)
    public Response login(@FormParam("user") String username,
                          @FormParam("password") String password,
                          @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == spawn.getWebPortSSL());
        try {
            String token = spawn.getPermissionsManager().login(username, password, usingSSL);
            Response.ResponseBuilder builder = Response.ok(token);
            builder.header("Access-Control-Allow-Origin",
                           "http://" + uriInfo.getAbsolutePath().getHost() +
                           ":" + spawn.getWebPort());
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            log.warn("Internal error in authentication attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity("internal error").build();
        }
    }

    @POST
    @Path("/validate")
    @Produces(MediaType.TEXT_PLAIN)
    public Response validate(@FormParam("user") String username,
                             @FormParam("token") String token,
                             @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == spawn.getWebPortSSL());
        try {
            User user = spawn.getPermissionsManager().authenticate(username, token);
            Response.ResponseBuilder builder = Response.ok(Boolean.toString(user != null));
            builder.header("Access-Control-Allow-Origin",
                           "http://" + uriInfo.getAbsolutePath().getHost() +
                           ":" + spawn.getWebPort());
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            log.warn("Internal error in validation attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity("internal error").build();
        }
    }

    @POST
    @Path("/sudo")
    @Produces(MediaType.TEXT_PLAIN)
    public Response sudo(@FormParam("user") String username,
                         @FormParam("password") String password,
                         @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == spawn.getWebPortSSL());
        try {
            String sudoToken = spawn.getPermissionsManager().sudo(username, password, usingSSL);
            Response.ResponseBuilder builder = Response.ok(sudoToken);
            builder.header("Access-Control-Allow-Origin",
                           "http://" + uriInfo.getAbsolutePath().getHost() +
                           ":" + spawn.getWebPort());
            builder.header("Access-Control-Allow-Methods", "POST");
            return builder.build();
        } catch (Exception ex)  {
            log.warn("Internal error in sudo attempt for user {} with ssl {}", username, usingSSL, ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @POST
    @Path("/logout")
    public void logout(@FormParam("user") String username,
                       @FormParam("token") String token,
                       @Context UriInfo uriInfo) {
        URI uri = uriInfo.getRequestUri();
        boolean usingSSL = (uri.getPort() == spawn.getWebPortSSL());
        try {
            spawn.getPermissionsManager().logout(username, token);
        } catch (Exception ex)  {
            log.warn("Internal error in logout attempt for user {} with ssl {}", username, usingSSL, ex);
        }
    }

}
