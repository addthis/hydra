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

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;

public class HtmlBundleEncoder extends AbstractBufferingHttpBundleEncoder {

    public HtmlBundleEncoder() {
        super();
        setContentTypeHeader(responseStart, "text/html; charset=utf-8");
        responseStart.headers().set("Access-Control-Allow-Origin", "*");
    }

    @Override
    protected void appendResponseStartToString(StringBuilder sendBuffer) {
        sendBuffer.append("<table border=1 cellpadding=1 cellspacing=0>\n");
    }

    @Override
    public void appendBundleToString(Bundle row, StringBuilder stringBuilder) {
        stringBuilder.append("<tr>");
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            stringBuilder.append("<td>").append(o).append("</td>");
        }
        stringBuilder.append("</tr>\n");
    }

    @Override
    protected void appendResponseEndToString(StringBuilder sendBuffer) {
        sendBuffer.append("</table>");
    }
}
