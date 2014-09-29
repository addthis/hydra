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
package com.addthis.hydra.task.stream;

import java.io.IOException;
import java.io.InputStream;

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.io.ByteStreams;

import org.apache.commons.lang3.RandomStringUtils;

import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;

public class TestStreamSourceMeshy {
    private static final Logger log = LoggerFactory.getLogger(TestStreamSourceMeshy.class);

    @Test @Ignore
    public void missingFileErrors() throws IOException {
        MeshyClient meshLink = new MeshyClient("localhost", 5000);
        Collection<FileReference> fileReferences = meshLink.listFiles(new String[]{"job0/*"});
//        log.info("file refs: {}", fileReferences);
        String meshUuid = fileReferences.iterator().next().getHostUUID();
        log.info("remote uuid: {}", meshUuid);

        InputStream inputStream = new StreamSource(meshLink, meshUuid,
                                                   "job0/notadir/notafile", 0).getInputStream();
        log.info(new String(ByteStreams.toByteArray(inputStream)));
        inputStream.close();
        meshLink.close();
    }

    @Test
    public void testStreamSourcesSort() {
        final StreamSourceMeshy source = new StreamSourceMeshy();
        final int numElements = 100000;

        FileReference[] fileRefs = new FileReference[numElements];
        List<MeshyStreamFile> streamFiles = new ArrayList<>(numElements);
        DateTime date = DateTime.now();

        try {
            for (int i = 0; i < numElements; i++) {
                fileRefs[i] = new FileReference("", 0, i);
                Field f = fileRefs[i].getClass().getDeclaredField("hostUUID");
                f.setAccessible(true);
                String uuid = RandomStringUtils.randomAlphabetic(20);
                f.set(fileRefs[i], uuid);
                streamFiles.add(new MeshyStreamFile(source, date, fileRefs[i]));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }

        Collections.shuffle(streamFiles);

        Collections.sort(streamFiles, new Comparator<MeshyStreamFile>() {
            @Override
            public int compare(MeshyStreamFile streamFile1, MeshyStreamFile streamFile2) {
                return source.compareStreamFiles(streamFile1, streamFile2);
            }
        });

    }

}
