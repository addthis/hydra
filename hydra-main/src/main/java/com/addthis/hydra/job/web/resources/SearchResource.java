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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.PipedInputStream;

import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.spawn.search.SearchOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/search")
public class SearchResource {
    private static final Logger log = LoggerFactory.getLogger(SearchResource.class);
    private final Spawn spawn;

    public SearchResource(Spawn spawn) {
        this.spawn = spawn;
    }

    @GET
    @Path("/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSearch(@QueryParam("q") String q) {
        try {
            final PipedInputStream results = spawn.getSearchResultStream(new SearchOptions(q));
            StreamingOutput stream = output -> {
                byte[] buf = new byte[256];
                int len;
                try {
                    while ((len = results.read(buf)) != -1) {
                        output.write(buf, 0, len);
                        output.flush();
                    }
                } finally {
                    results.close();
                }
            };
            return Response.ok().entity(stream).build();
        } catch (IOException e) {
            log.error("[search/all] IO error: {}", e.getMessage(), e);
            return Response.serverError().build();
        }

    }
}
