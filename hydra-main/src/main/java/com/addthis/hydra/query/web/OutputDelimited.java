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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.QueryException;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import io.netty.channel.ChannelHandlerContext;

class OutputDelimited extends AbstractHttpOutput {

    String delimiter;

    OutputDelimited(String filename, String delimiter) {
        super();
        this.delimiter = delimiter;
        setContentTypeHeader(response, "application/csv; charset=utf-8");
        response.headers().set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    public static OutputDelimited create(String filename, String format) {
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
                throw new QueryException("Invalid format");
        }
        if (!filename.toLowerCase().endsWith("." + format)) {
            filename = filename.concat("." + format);
        }
        return new OutputDelimited(filename, delimiter);
    }

    @Override
    public void send(ChannelHandlerContext ctx, Bundle row) {
        super.send(ctx, row);
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                ctx.write(delimiter);
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
                        ctx.write(o.toString());
                        break;
                    case STRING:
                        ctx.write("\"");
                        ctx.write(o.toString().replace('"', '\'').replace('\n', ' ').replace('\r', ' '));
                        ctx.write("\"");
                        break;
                    default:
                        break;
                }
            }
        }
        ctx.write("\n");
    }
}
