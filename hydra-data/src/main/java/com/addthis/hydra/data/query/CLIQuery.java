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
package com.addthis.hydra.data.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.List;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.hydra.data.util.Tokenizer;


/**
 * command-line operation processor for performing queries on unix streams cat
 * [file] | clop 'sort=0:s:a;merge=ksu' [in-sep] [out-sep] [grouping]
 */
public class CLIQuery {

    private static final boolean debug = Parameter.boolValue("cliquery.debug", false);

    public static void main(String args[]) throws Exception {
        if (args.length == 0) {
            System.out.println("usage: [ops] <input-separator> <output-separator> <grouping>");
            System.out.println("  this utility reads from stdin using newline for record delimiters.");
            System.out.println("  [ops] is the shorthand notation for pipelined query transforms.");
            System.out.println("  field separators can be specified for input and output and default to comma.");
            System.out.println("  grouping is possible if fields contain separators.  ex: (), {}, [], ''");
            return;
        }

        // setup result processor
        String insep = args.length > 1 ? args[1] : ",";
        String outsep = args.length > 2 ? args[2] : ",";
        String group[] = new String[]{"\"", "'", "()", "[]", "{}"};
        File tempDir = Files.createTempDir();
        QueryOpProcessor rp = new QueryOpProcessor.Builder(new CMDLineDataChannelOutput(outsep), args[0])
                .tempDir(tempDir).build();
        if (args.length > 3) {
            group = new String[args.length - 3];
            System.arraycopy(args, 3, group, 0, group.length);
        }

        if (debug) {
            System.out.println("ops -- " + rp.printOps());
        }

        // create tokenizer
        Tokenizer tk = new Tokenizer(insep, group, false);

        // setup input stream
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while ((line = br.readLine()) != null) {
            List<String> tok = tk.tokenize(line);
            if (tok == null || tok.size() == 0) {
                continue;
            }
            String[] cols = new String[tok.size()];
            for (int i = 0; i < tok.size(); i++) {
                cols[i] = "" + i;
            }
            Bundle b = new KVBundle();
            BundleFormat f = b.getFormat();
            int col = 0;
            for (String t : tok) {
                b.setValue(f.getField(Integer.toString(col++)), ValueFactory.create(t));
            }
            if (b != null) {
                rp.send(b);
            }
        }

        // pass input results to processor, it will print
        rp.sendComplete();
        Files.deleteDir(tempDir);
    }
}

/** */
class CMDLineDataChannelOutput implements DataChannelOutput {

    private final BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));
    private final KVBundleFormat format = new KVBundleFormat();
    private final String outsep;

    public CMDLineDataChannelOutput(String outsep) {
        this.outsep = outsep;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toString();
    }

    public void send(Bundle rl) throws DataChannelError {
        try {
            int tc = 0;
            for (BundleField bf : rl.getFormat()) {
                ValueObject qv = rl.getValue(bf);
                if (tc++ > 0) {
                    out.append(outsep);
                }
                if (qv == null) {
                    continue;
                }
                String sv = qv.toString();
                if (qv.getClass() == ValueString.class) {
                    if (sv.indexOf("\"") >= 0) {
                        sv = sv.replace("\"", "\\\"");
                    }
                    out.append("\"").append(sv).append("\"");
                } else {
                    out.append(sv);
                }
            }
            out.append("\n");
            out.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sendComplete() {
        try {
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Bundle createBundle() {
        return new KVBundle(format);
    }

    @Override
    public void sourceError(DataChannelError er) {
        throw new RuntimeException(er);
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }
}
