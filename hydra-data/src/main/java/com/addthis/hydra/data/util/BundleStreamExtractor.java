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
package com.addthis.hydra.data.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.zip.GZIPInputStream;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.io.DataChannelReader;

/**
 * reads persisted bundle streams (streamserver files, etc) and prints them
 * out in various formats.  primarily for debugging.
 */
@Deprecated
public class BundleStreamExtractor {

    public static void main(String args[]) throws Exception {
        String file = null;
        boolean asMap = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("file=")) {
                file = arg.substring(5);
            } else if (arg.equals("asmap")) {
                asMap = true;
            } else {
                file = arg;
            }
        }

        if (file == null) {
            throw new RuntimeException("file missing");
        }

        InputStream input = null;

        input = new FileInputStream(new File(file));
        if (file.endsWith(".gz")) {
            input = new GZIPInputStream(input);
        }

        DataChannelReader reader = new DataChannelReader(new ListBundle(), input);
        while (true) {
            try {
                Bundle bundle = reader.read();
                if (!asMap) {
                    System.out.println(bundle.toString());
                } else {
                    System.out.println(Bundles.getAsStringMapSlowly(bundle));
                }
            } catch (EOFException eof) {
                break;
            }
        }
    }
}
