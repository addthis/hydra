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

import java.io.File;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleOutput;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.channel.BlockingNullConsumer;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.data.tree.ReadTree;
import com.addthis.hydra.query.QueryEngineSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/**
 * should be abstracted further to be a generic pipe for queries and results.
 * submits and handles responses from QueryChannel.
 *
 * TODO implement interrupt/cancel
 */
public class QueryChannelUtil {

    private static final Logger log = LoggerFactory.getLogger(QueryChannelUtil.class);

    /**
     * @param args host=[host] port=[port] job=[job] path=[path] ops=[ops] lops=[lops] data=[datadir] [iter] [quiet] [sep=separator] [out=file] [trace] [param=val]
     */
    public static void main(String[] args) throws Exception {
        runQuery(args);
    }

    /** */
    private static final class PrintOp implements BundleOutput {

        public PrintOp(String sep, String out) throws java.io.FileNotFoundException {
            this.sep = sep;
            if (out == null) {
                sink = System.out;
            } else {
                sink = new PrintStream(out);
            }
        }

        private final AtomicInteger rowno = new AtomicInteger(0);
        private final String sep;
        private final PrintStream sink;

        @Override
        public void send(Bundle row) {
            if (sep == null) {
                sink.println(rowno.incrementAndGet() + " " + row);
                return;
            }
            int i = 0;
            for (BundleField field : row.getFormat()) {
                ValueObject o = row.getValue(field);
                if (i++ > 0) {
                    sink.print(sep);
                }
                if (o == null) {
                    continue;
                }
                if (o instanceof ValueDouble || o instanceof ValueLong) {
                    sink.print(o);
                } else {
                    sink.print("\"".concat(o.toString().replace('"', '\'')).concat("\""));
                }
            }
            sink.println();
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
        public void sendComplete() {
            sink.close();
        }
    }

    /** */
    private static void runQuery(String[] args) throws Exception {
        HashMap<String, String> qparam = new HashMap<>();
        String sep = null;
        boolean quiet = false;
        boolean traced = false;
        int iter = 1;
        ArrayList<String> paths = new ArrayList<>(1);
        ArrayList<String> ops = new ArrayList<>(1);
        ArrayList<String> lops = new ArrayList<>(1);
        String job = null;
        String data = null;
        String out = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            int eqpos;
            if (arg.equals("help")) {
                System.out.println("job=[job] path=[path] ops=[ops] lops=[lops] data=[datadir] [iter=#] [quiet] [sep=separator] [out=file] [trace] [param=val]");
                return;
            }
            if (arg.equals("trace")) {
                traced = true;
            } else if (arg.equals("quiet")) {
                quiet = true;
            } else if (arg.equals("csv")) {
                sep = ",";
            } else if (arg.equals("tsv")) {
                sep = "\t";
            } else if (arg.startsWith("sep=")) {
                sep = arg.substring(4);
            } else if (arg.startsWith("iter=")) {
                iter = Integer.parseInt(arg.substring(5));
            } else if (arg.startsWith("lops=")) {
                lops.add(arg.substring(5));
            } else if (arg.startsWith("ops=")) {
                ops.add(arg.substring(4));
            } else if (arg.startsWith("job=")) {
                job = arg.substring(4);
            } else if (arg.startsWith("path=")) {
                paths.add(arg.substring(5));
            } else if (arg.startsWith("fpath=")) {
                paths.add(LessBytes.toString(LessFiles.read(new File(arg.substring(6)))).trim());
            } else if (arg.startsWith("data=")) {
                data = arg.substring(5);
            } else if (arg.startsWith("out=")) {
                out = arg.substring(4);
            } else if ((eqpos = arg.indexOf("=")) > 0) {
                String key = arg.substring(0, eqpos);
                String val = arg.substring(eqpos + 1);
                qparam.put(key, val);
            }
        }
        Query query = new Query(job, paths.toArray(new String[paths.size()]), ops.toArray(new String[ops.size()]));
        query.setTraced(traced);
        for (Entry<String, String> e : qparam.entrySet()) {
            query.setParameter(e.getKey(), e.getValue());
        }
        if (!quiet) {
            System.out.println(">>> query " + query);
        }
        QuerySource client;
        if (data != null) {
            final File dir = new File(data);
            client = new QueryEngineSource() {
                @Override
                public QueryEngine getEngineLease() {
                    try {
                        return new QueryEngine(new ReadTree(dir));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } else {
            throw new RuntimeException("no data directory specified");
        }

        while (iter-- > 0) {
            long start = System.currentTimeMillis();
            File tempDir = LessFiles.createTempDir();
            BlockingNullConsumer consumer = new BlockingNullConsumer();
            QueryOpProcessor proc = new QueryOpProcessor.Builder(consumer, lops.toArray(new String[lops.size()]))
                    .tempDir(tempDir).build();
            proc.appendOp(new BundleOutputWrapper(new PrintOp(sep, out),
                    new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE)));
            client.query(query, proc);
            consumer.waitComplete();
            LessFiles.deleteDir(tempDir);
            if (!quiet) {
                System.out.println(">>> done " + proc + " in " + ((System.currentTimeMillis() - start) / 1000.0) + " sec");
            }
        }
        System.exit(0);
    }
}
