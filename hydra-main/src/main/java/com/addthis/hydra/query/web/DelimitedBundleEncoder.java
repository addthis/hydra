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

import java.util.ArrayList;
import java.util.List;

import java.nio.CharBuffer;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.QueryException;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.util.CharsetUtil;

class DelimitedBundleEncoder extends AbstractHttpBundleEncoder {

    String delimiter;

    List<Bundle> sendBuffer = new ArrayList<>(1000);

    DelimitedBundleEncoder(String filename, String delimiter) {
        super();
        this.delimiter = delimiter;
        setContentTypeHeader(responseStart, "application/csv; charset=utf-8");
        responseStart.headers().set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    private static ByteBuf encodeString(ByteBufAllocator alloc, CharSequence msg) {
        return ByteBufUtil.encodeString(alloc, CharBuffer.wrap(msg), CharsetUtil.UTF_8);
    }

    public static DelimitedBundleEncoder create(String filename, String format) {
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
        return new DelimitedBundleEncoder(filename, delimiter);
    }

    public static String buildRows(List<Bundle> rows, String delimiter) {
        int rowEstimate = (rows.get(0).getFormat().getFieldCount() * 12) + 1;
        StringBuilder stringBuilder = new StringBuilder(rowEstimate * rows.size());
        for (Bundle row : rows) {
            buildRow(row, delimiter, stringBuilder);
        }
        return stringBuilder.toString();
    }

    public static String buildRow(Bundle row, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder(row.getFormat().getFieldCount() * 12 + 1);
        buildRow(row, delimiter, stringBuilder);
        return stringBuilder.toString();
    }

    public static void buildRow(Bundle row, String delimiter, StringBuilder stringBuilder) {
        int count = 0;
        for (BundleField field : row.getFormat()) {
            ValueObject o = row.getValue(field);
            if (count++ > 0) {
                stringBuilder.append(delimiter);
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
                        stringBuilder.append(o.toString());
                        break;
                    case STRING:
                        stringBuilder.append("\"");
                        stringBuilder.append(o.toString().replace('"', '\'').replace('\n', ' ').replace('\r', ' '));
                        stringBuilder.append("\"");
                        break;
                    default:
                        break;
                }
            }
        }
        stringBuilder.append("\n");
    }

    @Override
    public void batchSend(ChannelHandlerContext ctx, List<Bundle> rows) {
        super.batchSend(ctx, rows);
        String batchString = buildRows(rows, delimiter);
        ByteBuf msg = encodeString(ctx.alloc(), batchString);
        ctx.writeAndFlush(new DefaultHttpContent(msg), ctx.voidPromise());
    }

    @Override
    public void sendComplete(ChannelHandlerContext ctx) {
        super.sendComplete(ctx);
        if (!sendBuffer.isEmpty()) {
            batchSend(ctx, sendBuffer);
        }
        sendBuffer.clear();
    }

    @Override
    public void send(ChannelHandlerContext ctx, Bundle row) {
        // don't call super here because our internal batching means batchSend will call it later
//        super.send(ctx, row);
        sendBuffer.add(row);
        if (sendBuffer.size() == 1000) {
            batchSend(ctx, sendBuffer);
            sendBuffer.clear();
        }
    }
}
