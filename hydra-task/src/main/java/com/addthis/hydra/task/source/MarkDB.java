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

import java.util.Map;
import java.util.TreeMap;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IReadWeighable;
import com.addthis.hydra.store.db.ReadPageDB;


public class MarkDB {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("MarkDB called without directory. Trying using the script in hydra-config/tools.");
            return;
        }
        File markDirFile = new File(args[0]);
        if (args.length == 2 && args[1].equals("-s")) {
            try {
                ReadPageDB<Record> markDB = new ReadPageDB<>(markDirFile, Record.class, 1000, 20);
                TreeMap<DBKey, Record> tm = markDB.toTreeMap();
                for (Map.Entry<DBKey, Record> se : tm.entrySet()) {
                    System.out.println("Path: " + se.getKey().rawKey() + " Index: " + se.getValue().index);
                }
                markDB.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to open database. Here is a stack trace.");
            }
        } else {
            try {
                ReadPageDB<Mark> markDB = new ReadPageDB<>(markDirFile, Mark.class, 1000, 20);
                TreeMap<DBKey, Mark> tm = markDB.toTreeMap();
                for (Map.Entry<DBKey, Mark> se : tm.entrySet()) {
                    System.out.println("Path: " + se.getKey().rawKey() + " Index: " + se.getValue().index);
                }
                markDB.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to open database. Here is a stack trace.");
            }
        }
    }

    public static final class Record implements Codec.Codable, IReadWeighable {

        public String val;
        public long index;
        public boolean end;

        private int weight = 0;

        @Override
        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public int getWeight() {
            return weight;
        }
    }

    public static final class Mark implements Codec.Codable, IReadWeighable {

        public String value;
        public long index;
        public int error;
        public boolean end;

        private int weight = 0;

        @Override
        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public int getWeight() {
            return weight;
        }
    }
}
