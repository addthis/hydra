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

import java.io.UncheckedIOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.jackson.Jackson;

import com.fasterxml.jackson.core.JsonProcessingException;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;

public class JsonBundleEncoder extends AbstractBufferingHttpBundleEncoder {

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
            if (o.getObjectType() == ValueObject.TYPE.CUSTOM) {
                o = o.asCustom().asSimple();
            }
            try {
                // TODO: no worse than the preivous version, but should still be able to avoid the string alloc.
                String encodedCopy = Jackson.defaultMapper().writeValueAsString(o.asNative());
                sendBuffer.append(encodedCopy);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        }
        sendBuffer.append(']');
    }

    @Override
    protected void appendResponseEndToString(StringBuilder sendBuffer) {
        sendBuffer.append("]");
    }
}
