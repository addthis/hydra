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
package com.addthis.hydra.query.util;

import java.io.IOException;

import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelReader;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;

import static com.addthis.meshy.MeshyClient.ListCallback;

/**
 * Date: 4/29/12
 * Time: 11:46 PM
 */
public class MeshQueryClient {

    private static final int maxQueryTime = Parameter.intValue("meshQueryClient.maxListFilesTime", 60);

    public static void main(String args[]) throws IOException {
        MeshyClient meshyClient = new MeshyClient(args[0], Integer.parseInt(args[1]));
        MeshQueryClient queryClient = new MeshQueryClient();
        try {
            DataChannelReader reader = queryClient.query(meshyClient, args[2], args[3], args[4]);
            if (reader == null) {
                System.out.println("no matching query source");
                return;
            }
            Bundle next = null;
            while ((next = reader.read()) != null) {
                System.out.println(next);
            }
        } finally {
            meshyClient.close();
        }
    }

    public DataChannelReader query(MeshyClient meshyClient, FileReference fileReference, String queryPath, String queryOps) {
        if (fileReference == null) {
            throw new DataChannelError("FileReference was null, unable to run query: " + queryPath);
        }
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("path", queryPath);
        options.put("ops", queryOps);
        try {
            return new FramedDataChannelReader(meshyClient.readFile(fileReference.getHostUUID(), fileReference.name, options), fileReference.name, 0);
        } catch (IOException e) {
            throw new DataChannelError(e);
        }
    }

    public DataChannelReader query(MeshyClient meshyClient, String prefix, String queryPath, String queryOps) {
        try {
            // sample of async / first-responder
            final Semaphore gate = new Semaphore(1);
            final AtomicReference<FileReference> fileRef = new AtomicReference<FileReference>(null);
            gate.acquire();
            meshyClient.listFiles(new String[]{prefix}, new ListCallback() {
                @Override
                public void receiveReference(FileReference ref) {
                    // only the first result will be set triggering a gate release
                    if (fileRef.compareAndSet(null, ref)) {
                        gate.release();
                    }
                }

                @Override
                public void receiveReferenceComplete() {
                    // if we get here it means no references were found
                    gate.release();
                }
            });
            // wait for first result / gate release
            gate.tryAcquire(maxQueryTime, TimeUnit.SECONDS);
            // open stream and return
            FileReference ref = fileRef.get();
            return query(meshyClient, ref, queryPath, queryOps);
        } catch (Exception ex) {
            throw new DataChannelError(ex);
        }
    }

}
