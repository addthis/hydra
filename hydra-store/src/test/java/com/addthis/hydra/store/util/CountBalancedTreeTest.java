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
package com.addthis.hydra.store.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.maljson.JSONArray;

import org.junit.Test;

public class CountBalancedTreeTest {

    @Test
    public void dummyTest() {
    }

    public static void main(String args[]) {
        CountBalancedTree<DBKey, Integer> at = new CountBalancedTree<DBKey, Integer>();
        JSONArray hist = new JSONArray();
        ArrayList<String> list = getList();
        Random r = new Random(1234);

        System.out.println("-- test seq insert --");
        int index = 0;
        for (String s : list) {
            DBKey dk = new DBKey(0, Raw.get(s));
            assert (at.put(dk, ++index) == null);
            assert (at.put(dk, index) == index);
            assert (at.get(dk) == index);
            hist.put(at.toJSON());
        }
        System.out.println("hist = " + hist.toString());
        dump(at);

        System.out.println("-- test rnd delete --");
        hist = new JSONArray();
        hist.put(at.toJSON());
        int size = at.size();
        while (list.size() > 0) {
            int pos = r.nextInt(list.size());
            try {
                assert (at.remove(list.remove(pos)) != null);
                assert (--size == at.size());
                hist.put(at.toJSON());
            } catch (Error ex) {
                ex.printStackTrace();
                System.out.println("fail @ pos=" + pos + " val=" + list.get(pos) + " list.size=" + list.size());
                break;
            }
        }
        hist.put(at.toJSON());
        System.out.println("hist = " + hist.toString());
        dump(at);
        at.clear();

        System.out.println("-- test rnd insert --");
        hist = new JSONArray();
        list = getList();
        size = 0;
        while (list.size() > 0) {
            int pos = r.nextInt(list.size());
            String s = list.remove(pos);
            DBKey dk = new DBKey(0, Raw.get(s));
            assert (at.put(dk, pos) == null);
            assert (at.get(dk) == pos);
            assert (++size == at.size());
            hist.put(at.toJSON());
        }
        System.out.println("hist = " + hist.toString());
        dump(at);

        System.out.println("-- test iter delete --");
        hist = new JSONArray();
        hist.put(at.toJSON());
        size = at.size();
        for (Iterator<?> i = at.entrySet().iterator(); i.hasNext(); ) {
            assert (i.next() != null);
            i.remove();
            assert (--size == at.size());
            hist.put(at.toJSON());
        }
        assert (at.size() == 0);
        System.out.println("hist = " + hist.toString());
        dump(at);

        System.out.println("-- test split --");
        at.clear();
        hist = new JSONArray();
        list = getList();
        while (list.size() > 0) {
            int pos = r.nextInt(list.size());
            String s = list.remove(pos);
            DBKey dk = new DBKey(0, Raw.get(s));
            assert (at.put(dk, pos) == null);
            assert (at.get(dk) == pos);
            hist.put(at.toJSON());
        }
        dump(at);
        CountBalancedTree<DBKey, Integer> split = at.split();
        hist.put(at.toJSON());
        dump(at);
        dump(split);

        System.out.println("-- massive random insert/delete --");
        size = 0;
        at.clear();
        list = getList();
        LinkedList<JSONArray> rr = new LinkedList<JSONArray>();
        rr.add(at.toJSON());
        for (int i = 0; i < 100000; i++) {
            try {
                int pos = r.nextInt(list.size());
                String s = list.get(pos);
                DBKey dk = new DBKey(0, Raw.get(s));
                if (at.put(dk, pos) == null) {
//System.out.println("put "+s+" @ "+pos);
                    assert (++size == at.size());
                }
                pos = r.nextInt(list.size());
                s = list.get(pos);
                dk = new DBKey(0, Raw.get(s));
                if (at.remove(dk) != null) {
//System.out.println("del "+s+" @ "+pos);
                    assert (--size == at.size());
                }
                if (i % 100 == 99 && at.size() > 3) {
                    CountBalancedTree<DBKey, Integer> bif = at.split();
                    assert (bif.size() + at.size() == size);
                    size = at.size();
                }
            } catch (Error ex) {
                System.out.println("ERR @ " + i + " at=" + at.toJSON());
                ex.printStackTrace();
                break;
            }
            rr.add(at.toJSON());
            if (rr.size() > 5) {
                rr.remove();
            }
        }
        rr.addFirst(at.toJSON());
        hist = new JSONArray();
        for (JSONArray a : rr) {
            hist.put(a);
        }
        System.out.println("hist = " + hist.toString());
    }

    private static ArrayList<String> getList() {
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < 64; i += 2) {
            list.add(new String(new char[]{(char) ('a' + (i / 16)), (char) ('a' + (i % 16))}));
        }
        return list;
    }

    private static void dump(CountBalancedTree<DBKey, Integer> at) {
        System.out.println("--- dump ---");
        for (DBKey i : at.keySet()) {
            System.out.print(i + " ");
        }
        System.out.println();
        DBKey bc = new DBKey(0, Raw.get("bc"));
        DBKey bd = new DBKey(0, Raw.get("bd"));
        System.out.println("floor bc = " + at.closest(at.getRootNode(), bc, true));
        System.out.println("floor bd = " + at.closest(at.getRootNode(), bd, true));
        System.out.println("ceiling bc = " + at.closest(at.getRootNode(), bc, false));
        System.out.println("ceiling bd = " + at.closest(at.getRootNode(), bd, false));
        System.out.println("firstKey = " + at.firstKey());
        System.out.println("lastKey = " + at.lastKey());
        System.out.println(at.debug(false));
    }
}
