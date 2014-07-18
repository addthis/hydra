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

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.source.bundleizer.Bundleizer;
import com.addthis.hydra.task.source.bundleizer.BundleizerFactory;

import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;

public class DataSourceHttp extends TaskDataSource {

    @FieldConfig(required = true) private BundleizerFactory format;
    @FieldConfig(required = true) private String url;
    @FieldConfig(required = true) private ConfigValue content;

    private Bundleizer bundleizer;
    private Bundle nextBundle;
    private InputStream underlyingInputStream;

    @Override public void init(TaskRunConfig config) {
        try {
            URL url = new URL(this.url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            String configAsJsonString = content.render(ConfigRenderOptions.concise());
            try (OutputStream os = conn.getOutputStream()) {
                os.write(configAsJsonString.getBytes());
                os.flush();
            }

            underlyingInputStream = conn.getInputStream();
            bundleizer = format.createBundleizer(underlyingInputStream, new ListBundle());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public Bundle next() throws DataChannelError {
        if (nextBundle != null) {
            nextBundle = null;
            return nextBundle;
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
