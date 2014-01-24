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
package com.addthis.hydra.task.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.zip.GZIPInputStream;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.stream.StreamSource;

/**
 * Date: 5/9/12
 * Time: 6:02 PM
 */
public class BundleStreamPeeker {

    public static void main(String args[]) throws Exception {
        Meshy meshy = null;
        String file = null;
        String mesh = null;
        boolean asMap = false;
        int sample = 10;

        for (String arg : args) {
            if (arg.startsWith("file=")) {
                file = arg.substring(5);
            } else if (arg.startsWith("mesh=")) {
                mesh = arg.substring(5);
            } else if (arg.startsWith("sample=")) {
                sample = Integer.parseInt(arg.substring(7));
            } else if ("asmap".equals(arg)) {
                asMap = true;
            } else {
                file = arg;
            }
        }

        if (file == null) {
            throw new RuntimeException("file missing");
        }

        InputStream input = null;

        try {

            if (mesh != null) {
                String hostPort[] = Strings.splitArray(mesh, ":");
                meshy = new MeshyClient(hostPort[0], Integer.parseInt(hostPort[1]));
                FileSource src = new FileSource(meshy, new String[]{file});
                src.waitComplete();
                src.getFileList();
                FileReference ref = null;
                for (FileReference fref : src.getFileList()) {
                    ref = fref;
                    break;
                }
                if (ref == null) {
                    throw new RuntimeException("no mesh files matched: " + file);
                }
                input = new StreamSource(meshy, ref.getHostUUID(), ref.name, 0).getInputStream();
            } else {
                input = new FileInputStream(new File(file));
            }

            if (file.endsWith(".gz")) {
                input = new GZIPInputStream(input);
            }
            DataChannelReader reader = new DataChannelReader(new ListBundle(), input);
            while (sample-- > 0) {
                try {
                    Bundle bundle = reader.read();
                    if (asMap) {
                        System.out.println(Bundles.getAsStringMapSlowly(bundle));
                    } else {
                        System.out.println(bundle.toString());
                    }
                } catch (EOFException eof) {
                    break;
                }
            }

        } finally {
            if (meshy != null) {
                meshy.close();
            }
        }
    }
}
