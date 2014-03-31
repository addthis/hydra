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

class OutputHTML extends ServletConsumer {

    OutputHTML(HttpServletResponse response) throws IOException, InterruptedException {
        super(response);
        response.setContentType("text/html; charset=utf-8");
        writer.write("<table border=1 cellpadding=1 cellspacing=0>\n");
    }

    @Override
    public synchronized void send(Bundle row) {
        writer.write("<tr>");
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            writer.write("<td>" + o + "</td>");
        }
        writer.write("</tr>\n");
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
        writer.write("</table>");
        QueryServlet.queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        setDone();
    }
}
