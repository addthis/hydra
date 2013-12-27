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
import java.io.IOException;
import java.io.PrintStream;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleOutput;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.channel.BlockingBufferedConsumer;
import com.addthis.hydra.data.channel.BlockingNullConsumer;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryEngine;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.channel.QueryChannelClient;
import com.addthis.hydra.data.query.channel.QueryChannelClientX;
import com.addthis.hydra.data.query.channel.QueryHost;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.query.source.QuerySource;
import com.addthis.hydra.data.tree.ReadTree;
import com.addthis.hydra.query.QueryEngineSource;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * should be abstracted further to be a generic pipe for queries and results.
 * submits and handles responses from QueryChannel.
 *
 * TODO implement interrupt/cancel
 */
public class QueryChannelUtil {

    private static final Logger log = LoggerFactory.getLogger(QueryChannelUtil.class);

    private static final Map<QueryHost, QueryChannelClient> queryChannelClientMap = new HashMap<QueryHost, QueryChannelClient>();
    private static final Lock clientMapLock = new ReentrantLock();

    /**
     * sync query call helper
     */
    public static DataTable syncQuery(QueryHost host, Query query) throws Exception {
        try {
            QueryChannelClient client = getQueryChannelClient(host);
            BlockingBufferedConsumer consumer = new BlockingBufferedConsumer();
            QueryOpProcessor proc = Query.createProcessor(consumer);
            client.query(query, proc);
            return consumer.getTable();
        } catch (Exception e) {
            removeQueryChannelClient(host);
            throw new Exception("Exception running syncQuery", e);
        }
    }

    private static QueryChannelClient getQueryChannelClient(QueryHost host) throws IOException {
        QueryChannelClient client = null;
        clientMapLock.lock();
        try {
            client = queryChannelClientMap.get(host);
            if (client == null) {
                client = new QueryChannelClient(host);
                queryChannelClientMap.put(host, client);
            }
        } finally {
            clientMapLock.unlock();
        }
        return client;
    }


    private static void removeQueryChannelClient(QueryHost host) {
        clientMapLock.lock();
        try {
            queryChannelClientMap.remove(host);
            log.warn("removed QueryChannelClient for host: " + host);
        } finally {
            clientMapLock.unlock();
        }
    }

    /**
     * example of how to make simple async calls
     */
    public static QueryHandle asyncQuery(QueryHost host, Query query, DataChannelOutput consumer) throws Exception {
        return getQueryChannelClient(host).query(query, consumer);
    }

    /**
     * @param args host=[host] port=[port] job=[job] path=[path] ops=[ops] rops=[rops] lops=[lops] data=[datadir] [iter] [quiet] [sep=separator] [out=file] [trace] [param=val]
     */
    public static void main(String args[]) throws Exception {
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
    private static void runQuery(String args[]) throws Exception {
        HashMap<String, String> qparam = new HashMap<String, String>();
        String sep = null;
        boolean quiet = false;
        boolean traced = false;
        boolean dsortcompression = false;
        long ttl = 0;
        int iter = 1;
        String path = null;
        String job = null;
        String lops = null;
        String ops = null;
        String host = "localhost";
        String data = null;
        String out = null;
        List<QueryHost> hosts = new LinkedList<QueryHost>();
        int port = 2601;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            int eqpos = 0;
            if (arg.equals("help")) {
                System.out.println("host=[host] port=[port] job=[job] path=[path] ops=[ops] rops=[rops] lops=[lops] data=[datadir] [iter=#] [quiet] [sep=separator] [out=file] [trace] [param=val]");
                return;
            }
            if (arg.startsWith("host=")) {
                host = arg.substring(5);
            } else if (arg.startsWith("hosts=")) {
                for (String hostport : Strings.splitArray(arg.substring(6), ",")) {
                    String hp[] = Strings.splitArray(hostport, ":");
                    hosts.add(new QueryHost(hp[0], hp.length > 1 ? Integer.parseInt(hp[1]) : port));
                }
            } else if (arg.startsWith("port=")) {
                port = Integer.parseInt(arg.substring(5));
            } else if (arg.equals("trace")) {
                traced = true;
            } else if (arg.equals("quiet")) {
                quiet = true;
            } else if (arg.equals("dsortcompression")) {
                dsortcompression = true;
            } else if (arg.equals("csv")) {
                sep = ",";
            } else if (arg.equals("tsv")) {
                sep = "\t";
            } else if (arg.startsWith("sep=")) {
                sep = arg.substring(4);
            } else if (arg.startsWith("iter=")) {
                iter = Integer.parseInt(arg.substring(5));
            } else if (arg.startsWith("lops=")) {
                lops = arg.substring(5);
            } else if (arg.startsWith("ops=")) {
                ops = arg.substring(4);
            } else if (arg.startsWith("job=")) {
                job = arg.substring(4);
            } else if (arg.startsWith("path=")) {
                path = arg.substring(5);
            } else if (arg.startsWith("fpath=")) {
                path = Bytes.toString(Files.read(new File(arg.substring(6)))).trim();
            } else if (arg.startsWith("ttl=")) {
                ttl = Long.parseLong(arg.substring(4));
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
        Query query = new Query(job, path, ops);
        query.setTraced(traced);
        query.setCacheTTL(ttl);
        if (dsortcompression) {
            query.setParameter("dsortcompression", "true");
        }
        for (Entry<String, String> e : qparam.entrySet()) {
            query.setParameter(e.getKey(), e.getValue());
        }
        if (!quiet) {
            System.out.println(">>> query " + query);
        }
        QuerySource client = null;
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
        } else if (hosts.size() > 0) {
            final boolean logit = log.isDebugEnabled() && !quiet;
            client = new QueryChannelClientX(hosts) {
                @Override
                public QuerySource leaseClient(QueryHost host) {
                    try {
                        if (logit) {
                            log.warn("lease client : " + host);
                        }
                        return new QueryChannelClient(host);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void releaseClient(QuerySource channel) {
                    if (logit) {
                        log.warn("release client : " + channel);
                    }
                }
            };
        } else {
            client = new QueryChannelClient(host, port);
        }

        while (iter-- > 0) {
            long start = System.currentTimeMillis();
            File tempDir = Files.createTempDir();
            BlockingNullConsumer consumer = new BlockingNullConsumer();
            QueryOpProcessor proc = Query.createProcessor(consumer).parseOps(lops).setTempDir(tempDir);
            proc.appendOp(new BundleOutputWrapper(new PrintOp(sep, out)));
            client.query(query, proc);
            consumer.waitComplete();
            Files.deleteDir(tempDir);
            if (!quiet) {
                System.out.println(">>> done " + proc + " in " + ((System.currentTimeMillis() - start) / 1000.0) + " sec");
            }
        }
        System.exit(0);
    }
}
