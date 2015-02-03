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
package com.addthis.hydra.task.source;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.Map;
import java.util.TreeMap;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IReadWeighable;
import com.addthis.hydra.store.db.ReadPageDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class MarkDB {
    private static final Logger log = LoggerFactory.getLogger(MarkDB.class);

    public static void main(String... args) {
        log.info("args.length {}, args {}", args.length, args);
        if (args.length == 0) {
            printMarks(ReadMark.class, new File("."));
        } else if ((args.length == 1) && ("-h".equals(args[0]) || "--help".equals(args[0]))) {
            printUsage();
        } else if ((args.length == 1) && "-s".equals(args[0])) {
            printMarks(ReadSimpleMark.class, new File("."));
        } else if (args.length == 1) {
            printMarks(ReadMark.class, new File(args[0]));
        } else if ((args.length == 2) && "-s".equals(args[0])) {
            printMarks(ReadSimpleMark.class, new File(args[1]));
        } else {
            printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: [-s] [directory]\nDump file marks to standard output.\n  -s    Use legacy format");
    }

    private static <T extends SimpleMark & IReadWeighable> void printMarks(Class<T> markClass, File directory) {
        try (ReadPageDB<T> markDB = new ReadPageDB<>(directory, markClass, 1000, 20)) {
            TreeMap<DBKey, T> tm = markDB.toTreeMap();
            for (Map.Entry<DBKey, T> se : tm.entrySet()) {
                System.out.println("Path: " + se.getKey().rawKey() + " Index: " + se.getValue().getIndex());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class ReadSimpleMark extends SimpleMark implements IReadWeighable {
        private int weight;

        @Override public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override public int getWeight() {
            return weight;
        }
    }

    public static class ReadMark extends Mark implements IReadWeighable {
        private int weight;

        @Override public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override public int getWeight() {
            return weight;
        }
    }
}
