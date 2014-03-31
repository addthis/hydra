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

package com.addthis.hydra.query.web;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;

class OutputJson extends ServletConsumer {

    int rows = 0;
    private String jsonp;

    OutputJson(HttpServletResponse response, String jsonp, String jargs) throws IOException, InterruptedException {
        super(response);
        this.jsonp = jsonp;
        response.setContentType("application/json; charset=utf-8");
        if (jsonp != null) {
            writer.write(jsonp);
            writer.write("(");
            if (jargs != null) {
                writer.write(jargs);
                writer.write(",");
            }
        }
        writer.write("[");
    }

    @Override
    public synchronized void send(Bundle row) {
        if (rows++ > 0) {
            writer.write(",");
        }
        writer.write("[");
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                writer.write(",");
            }
            if (o == null) {
                continue;
            }
            ValueObject.TYPE type = o.getObjectType();
            if (type == ValueObject.TYPE.CUSTOM) {
                o = o.asCustom().asSimple();
                type = o.getObjectType();
            }
            switch (type) {
                case INT:
                case FLOAT:
                    writer.write(o.toString());
                    break;
                case STRING:
                    writer.write('"');
                    writer.write(QueryServlet.jsonEncode(o.toString()));
                    writer.write('"');
                    break;
                default:
                    break;
            }
        }
        writer.write("]");
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override
    public void sendComplete() {
        writer.write("]");
        if (jsonp != null) {
            writer.write(");");
        }
        QueryServlet.queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        setDone();
    }
}
