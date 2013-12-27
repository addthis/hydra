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
package com.addthis.hydra.task.output;

import java.io.File;
import java.io.IOException;

import java.util.List;

import com.addthis.basis.util.RollingLog;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.codec.Codec;

public class ValuesOutputFile extends ValuesOutput {

    @Codec.Set(codable = true, required = true)
    private ValuesStreamFormatter format;
    @Codec.Set(codable = true)
    private String dir;
    @Codec.Set(codable = true)
    private String prefix = "values";
    @Codec.Set(codable = true)
    private String suffix;
    @Codec.Set(codable = true)
    private boolean compress;
    @Codec.Set(codable = true)
    private long maxSize;
    @Codec.Set(codable = true)
    private long maxAge;

    private RollingLog log;

    public void close() {
        try {
            log.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(Bundle row) {
        try {
            format.output(row);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public void open() throws Exception {
        log = new RollingLog(new File(dir), prefix, suffix, compress, maxSize, maxAge);
        format.init(log);
    }

    @Override
    public void sourceError(DataChannelError er) {
        // TODO Auto-generated method stub
    }

    @Override
    public void sendComplete() {
        close();
    }

    @Override
    public Bundle createBundle() {
        // TODO Auto-generated method stub
        return null;
    }
}
