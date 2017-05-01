package com.addthis.hydra.meshy.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;
import java.util.function.Function;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.StreamSource;

public class MeshConnection {
    private MeshyClient client;

    public MeshConnection(String host, int port) throws IOException {
        client = new MeshyClient(host, port);
    }

    public Collection<FileReference> list(String path) throws IOException {
        return client.listFiles(new String[] {path});
    }

    public void streamFile(
            String uuid,
            String path,
            OutputStream outputStream,
            Function<InputStream, ? extends InputStream> inputTransformer) throws IOException {
        StreamSource source = new StreamSource(client, uuid, path, 0);
        try (InputStream in = inputTransformer.apply(source.getInputStream())) {
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
}
