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
package com.addthis.hydra.job.spawn.resources;

import javax.servlet.http.HttpServlet;

import java.util.HashSet;
import java.util.Set;

import com.sun.jersey.api.core.ScanningResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpawnConfig extends ScanningResourceConfig {

    private Set<HttpServlet> servlets;

    public SpawnConfig() {
        servlets = new HashSet<HttpServlet>();
        //Automatic json conversion from resources
        getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    }

    public void addResource(Object resource) {
        getSingletons().add(checkNotNull(resource));
    }

//  public void addServlet(HttpServlet servlet)
//  {
//      //get
//      //this.servlets.add(servlet);
//      super.g
//  }

    public void addProvider(Object provider) {
        this.getSingletons().add(checkNotNull(provider));
    }

    public void addProvider(Class klass) {
        this.getClasses().add(checkNotNull(klass));
    }

    public Set<HttpServlet> getServlets() {
        return servlets;
    }
}
