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

public class JsonBundleEncoder extends AbstractBufferingHttpBundleEncoder {

    static final char[] hex = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public JsonBundleEncoder() {
        super();
        setContentTypeHeader(responseStart, "application/json; charset=utf-8");
        responseStart.headers().set("Access-Control-Allow-Origin", "*");
    }

    @Override
    public void appendResponseStartToString(StringBuilder sendBuffer) {
        sendBuffer.append('[');
    }

    @Override
    public void appendBundleToString(Bundle row, StringBuilder sendBuffer) {
        sendBuffer.append(',');
        appendInitialBundleToString(row, sendBuffer);
    }

    @Override
    protected void appendInitialBundleToString(Bundle firstRow, StringBuilder sendBuffer) {
        sendBuffer.append('[');
        int count = 0;
        for (BundleField field : firstRow.getFormat()) {
            ValueObject o = firstRow.getValue(field);
            if (count++ > 0) {
                sendBuffer.append(',');
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
                    sendBuffer.append(o.toString());
                    break;
                case STRING:
                    sendBuffer.append('"');
                    sendBuffer.append(jsonEncode(o.toString()));
                    sendBuffer.append('"');
                    break;
                default:
                    break;
            }
        }
        sendBuffer.append(']');
    }

    @Override
    protected void appendResponseEndToString(StringBuilder sendBuffer) {
        sendBuffer.append("]");
    }

    /* convert string to json valid format */
    @VisibleForTesting
    static String jsonEncode(String s) {
        char[] ca = s.toCharArray();
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
                            char[] cb = new char[4];
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
