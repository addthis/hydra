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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import io.netty.channel.ChannelHandlerContext;

class OutputHTML extends AbstractHttpOutput {

    OutputHTML(ChannelHandlerContext ctx) {
        super(ctx);
        setContentTypeHeader(response, "text/html; charset=utf-8");
        ctx.write("<table border=1 cellpadding=1 cellspacing=0>\n");
    }

    @Override
    public void writeStart() {
        super.writeStart();
        ctx.write("<table border=1 cellpadding=1 cellspacing=0>\n");
    }

    @Override
    public synchronized void send(Bundle row) {
        ctx.write("<tr>");
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            ctx.write("<td>" + o + "</td>");
        }
        ctx.write("</tr>\n");
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
        ctx.write("</table>");
        HttpQueryCallHandler.queryTimes.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        super.sendComplete();
    }
}
