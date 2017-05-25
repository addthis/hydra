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
package com.addthis.hydra.meshy.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeshConnection {
    private static Logger log = LoggerFactory.getLogger(MeshConnection.class);
    private MeshyClient client;
    private final int bytesToScanForBinary = Parameter.intValue("mesh.http.binary.bytes.scan", 4096);
    private static final int MAX_BINARY_BYTE = 0x1F;
    // index of unsigned bytes that look binary to google chrome
    // see kByteLooksBinary in https://src.chromium.org/viewvc/chrome/trunk/src/net/base/mime_sniffer.cc
    private static final boolean[] BINARY_BYTE =
            {true, true, true, true, true, true, true, true, true, false, false, true, false, false, true, true, true,
             true, true, true, true, true, true, true, true, true, true, false, true, true, true, true};

    public MeshConnection(String host, int port) throws IOException {
        client = new MeshyClient(host, port);
    }

    public Collection<FileReference> list(String path) {
        try {
            return client.listFiles(new String[]{path});
        } catch (IOException e) {
            log.error("Failed to list files", e);
            return Collections.emptyList();
        }
    }

    /**
     * We don't catch IOException because we're reading and writing from 2 separate streams,
     * so distinguishing between their errors would require multiple try blocks, making the code too complex
     */
    public void streamFile(
            String uuid,
            String path,
            OutputStream outputStream,
            Function<InputStream, ? extends InputStream> transform) throws IOException {
        StreamSource source = new StreamSource(client, uuid, path, 0);
        try (InputStream in = transform.apply(source.getInputStream())) {
            byte[] buffer = new byte[4096];
            int read = 0;
            while ((read = in.read(buffer)) >= 0) {
                if (read == 0) {
                    continue;
                }
                if (read < 0) {
                    break;
                }
                outputStream.write(buffer, 0, read);
            }
        }
    }

    public boolean mightBeBinaryFile(String uuid, String path, Function<InputStream, ? extends InputStream> transform) {
        try {
            StreamSource source = new StreamSource(client, uuid, path, 0);
            try (InputStream in = transform.apply(source.getInputStream())) {
                byte[] buffer = new byte[4096];
                int read = 0;
                int scanned = 0;
                while (((read = in.read(buffer)) >= 0) && (scanned < bytesToScanForBinary)) {
                    scanned += read;
                    if (read == 0) {
                        continue;
                    }
                    if (read < 0) {
                        break;
                    }
                    if (mightBeBinaryBytes(buffer, read)) {
                        return true;
                    }
                }
                return false;
            }
        } catch (IOException e) {
            log.error("Error reading file to detect binary data", e);
            // we'll just say it's binary if there's an error.
            return true;
        }
    }

    private boolean mightBeBinaryBytes(byte[] buffer, int read) {
        for (int i = 0; i < read; i++) {
            int b = Byte.toUnsignedInt(buffer[i]);
            if ((b <= MAX_BINARY_BYTE) && BINARY_BYTE[b]) {
                return true;
            }
        }
        return false;
    }
}
