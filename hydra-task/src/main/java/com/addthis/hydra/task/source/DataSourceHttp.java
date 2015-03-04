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
package com.addthis.hydra.task.source;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.task.source.bundleizer.Bundleizer;
import com.addthis.hydra.task.source.bundleizer.BundleizerFactory;

import com.google.common.base.Throwables;
import com.google.common.escape.Escaper;
import com.google.common.io.ByteStreams;
import com.google.common.net.UrlEscapers;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceHttp extends TaskDataSource {

    static final int LOG_TRUNCATE_CHARS = 500;

    static final Logger log = LoggerFactory.getLogger(DataSourceHttp.class);

    @FieldConfig(required = true) private BundleizerFactory format;
    @FieldConfig(required = true) private String url;
    @FieldConfig(required = true) private JsonNode data;
    @FieldConfig(required = true) private Map<String, String> params = new HashMap<>();
    @FieldConfig(required = true) private String contentType;

    private Bundleizer bundleizer;
    private Bundle nextBundle;
    private InputStream underlyingInputStream;

    @Override public void init() {
        HttpURLConnection conn = null;
        try {
            StringBuilder urlMaker = new StringBuilder(url);
            if (!params.isEmpty()) {
                Escaper escaper = UrlEscapers.urlPathSegmentEscaper();
                urlMaker.append('?');
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    urlMaker.append(escaper.escape(entry.getKey()));
                    urlMaker.append('=');
                    urlMaker.append(escaper.escape(entry.getValue()));
                    urlMaker.append('&');
                }
            }
            URL javaUrl = new URL(urlMaker.toString());
            conn = (HttpURLConnection) javaUrl.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            if (!data.isNull()) {
                writeData(conn);
            }
            underlyingInputStream = conn.getInputStream();
            bundleizer = format.createBundleizer(underlyingInputStream, new ListBundle());
        } catch (Exception outer) {
            if (conn != null && conn.getErrorStream() != null) {
                try {
                    log.error("URL connection was unsuccessful. Response is {}",
                              new String(ByteStreams.toByteArray(conn.getErrorStream())));
                } catch (IOException inner) {
                    log.error("During connection error failure to read error stream: ", inner);
                }
            }
            throw Throwables.propagate(outer);
        }
    }

    private void writeData(HttpURLConnection conn) throws IOException {
        conn.setRequestProperty("Content-Type", contentType);
        try (OutputStream os = conn.getOutputStream()) {
            switch (contentType) {
                case "application/json":
                    Jackson.defaultMapper().writeValue(os, data);
                    break;
                case "application/x-www-form-urlencoded": {
                    Escaper escaper = UrlEscapers.urlFormParameterEscaper();
                    StringBuilder content = new StringBuilder();
                    Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        content.append(escaper.escape(field.getKey()));
                        content.append("=");
                        content.append(escaper.escape(field.getValue().asText()));
                        if (fields.hasNext()) {
                            content.append("&");
                        }
                    }
                    String contentString = content.toString();
                    log.info("First {} characters of POST body are {}", LOG_TRUNCATE_CHARS,
                             Strings.trunc(contentString, LOG_TRUNCATE_CHARS));
                    os.write(contentString.getBytes());
                    break;
                }
                default:
                    throw new IllegalStateException("Unknown content type " + contentType);
            }
            os.flush();
        }
    }

    @Override public Bundle next() throws DataChannelError {
        if (nextBundle != null) {
            Bundle result = nextBundle;
            nextBundle = null;
            return result;
        } else {
            try {
                return bundleizer.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override public Bundle peek() throws DataChannelError {
        if (nextBundle == null) {
            try {
                nextBundle = bundleizer.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return nextBundle;
    }

    @Override public void close() {
        try {
            underlyingInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
