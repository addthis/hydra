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

class OutputDelimited extends ServletConsumer {

    String delimiter;

    OutputDelimited(HttpServletResponse response, String filename, String delimiter) throws IOException, InterruptedException {
        super(response);
        this.delimiter = delimiter;
        response.setContentType("application/csv; charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    public static OutputDelimited create(HttpServletResponse response, String filename, String format) throws IOException, InterruptedException {
        String delimiter;
        switch (format) {
            case "tsv":
                delimiter = "\t";
                break;
            case "csv":
                delimiter = ",";
                break;
            case "psv":
                delimiter = "|";
                break;
            default:
                return null;
        }
        if (!filename.toLowerCase().endsWith("." + format)) {
            filename = filename.concat("." + format);
        }
        return new OutputDelimited(response, filename, delimiter);
    }

    @Override
    public synchronized void send(Bundle row) {
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                writer.write(delimiter);
            }
            if (o != null) {
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
                        writer.write(o.toString().replace('"', '\'').replace('\n', ' ').replace('\r', ' '));
                        writer.write('"');
                        break;
                    default:
                        break;
                }
            }
        }
        writer.write("\n");
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
        QueryServlet.queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        setDone();
    }
}
