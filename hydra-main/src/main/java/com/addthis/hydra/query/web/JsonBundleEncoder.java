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

import com.google.common.annotations.VisibleForTesting;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import io.netty.channel.ChannelHandlerContext;

class JsonBundleEncoder extends AbstractHttpBundleEncoder {

    static final char[] hex = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    int rows = 0;
    private final String jsonp;
    private final String jargs;

    JsonBundleEncoder(String jsonp, String jargs) {
        super();
        this.jsonp = jsonp;
        this.jargs = jargs;
        setContentTypeHeader(responseStart, "application/json; charset=utf-8");
        responseStart.headers().set("Access-Control-Allow-Origin", "*");
    }

    @Override
    public void writeStart(ChannelHandlerContext ctx) {
        super.writeStart(ctx);
        StringBuilder stringBuilder = new StringBuilder(3);
        if (jsonp != null) {
            stringBuilder.append(jsonp);
            stringBuilder.append('(');
            if (jargs != null) {
                stringBuilder.append(jargs);
                stringBuilder.append(',');
            }
        }
        stringBuilder.append('[');
        ctx.write(stringBuilder.toString());
    }

    @Override
    public void send(ChannelHandlerContext ctx, Bundle row) {
        super.send(ctx, row);
        StringBuilder stringBuilder = new StringBuilder(row.getFormat().getFieldCount() * 13 + 3);
        if (rows++ > 0) {
            stringBuilder.append(',');
        }
        stringBuilder.append('[');
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                stringBuilder.append(',');
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
                    stringBuilder.append(o.toString());
                    break;
                case STRING:
                    stringBuilder.append('"');
                    stringBuilder.append(jsonEncode(o.toString()));
                    stringBuilder.append('"');
                    break;
                default:
                    break;
            }
        }
        stringBuilder.append(']');
        ctx.writeAndFlush(stringBuilder.toString());
    }

    @Override
    public void sendComplete(ChannelHandlerContext ctx) {
        super.sendComplete(ctx);
        ctx.write("]");
        if (jsonp != null) {
            ctx.write(");");
        }
    }

    /* convert string to json valid format */
    @VisibleForTesting
    static String jsonEncode(String s) {
        char ca[] = s.toCharArray();
        int alt = 0;
        for (int i = 0; i < ca.length; i++) {
            if (ca[i] < 48 || ca[i] > 90) {
                alt++;
            }
        }
        if (alt == 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(ca.length + alt * 3);
        for (int i = 0; i < ca.length; i++) {
            char c = ca[i];
            if (c > 47 && c < 91) {
                sb.append(ca[i]);
            } else {
                switch (ca[i]) {
                    case '"':
                        sb.append("\\\"");
                        break;
                    case '\\':
                        sb.append("\\\\");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    default:
                        if (c < 32 || c > 255) {
                            sb.append("\\u");
                            long v = c;
                            char cb[] = new char[4];
                            for (int j = 0; j < 4; j++) {
                                cb[3 - j] = hex[(int) (v & 0xf)];
                                v >>= 4;
                            }
                            sb.append(cb);
                        } else {
                            sb.append(c);
                        }
                        break;
                }
            }
        }
        return sb.toString();
    }
}
