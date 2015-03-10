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
package com.addthis.hydra.data;

import java.io.File;

import com.addthis.basis.util.LessFiles;

import com.addthis.maljson.JSONArray;

import com.clearspring.analytics.stream.membership.BloomFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * <h1>MakeBloom</h1>
 * <p/>
 * <p>Create a Bloom Filter out of a string list and export it.</p>
 *
 */
public class MakeBloom {

    /**  */
    private static final Logger log = LoggerFactory.getLogger(MakeBloom.class);
    private static final String fpRate = System.getProperty("fpRate", "0.001");
    private static final double fp_rate = Double.parseDouble(fpRate);

    // get all the quoted words from a "frag" file
    public static String[] getWords(File in) throws java.io.IOException,
                                                    com.addthis.maljson.JSONException {
        log.debug("Reading " + in.length() + " bytes from [" + in + "]");
        JSONArray words = new JSONArray("[" + new String(LessFiles.read(in), "utf8") + "]");
        log.debug("Read " + words.length() + " words from [" + in + "]");
        String[] ret = new String[words.length()];
        for (int i = 0; i < words.length(); i++) {
            ret[i] = words.getString(i);
        }
        return ret;
    }

    public static void main(String[] args) throws java.io.IOException,
                                                  com.addthis.maljson.JSONException {
        if (args.length != 1 && args.length != 2) {
            throw new IllegalArgumentException("usage: MakeBloom word-list-file [bloom-file]");
        }

        File in = new File(args[0]);
        String[] words = getWords(in);

        BloomFilter bf = new BloomFilter(words.length, fp_rate);
        log.debug("Created: BloomFilter(" + bf.buckets() + " buckets, " +
                 bf.getHashCount() + " hashes); FP rate = " + fp_rate);
        for (int i = 0; i < words.length; i++) {
            bf.add(words[i]);
        }
        log.debug("Added words");

        File out = args.length == 2 ? new File(args[1]) :
                   LessFiles.replaceSuffix(in, "-" + words.length + "-" + fpRate + ".bloom");
        log.debug("Writing [" + out + "]");
        LessFiles.write(out, org.apache.commons.codec.binary.Base64.encodeBase64(BloomFilter.serialize(bf)), false);
        log.debug("Wrote " + out.length() + " bytes to [" + out + "]");
    }
}
