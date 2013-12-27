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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.data.query.QueryElement.ReferencePathIterator;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeActor;
import com.addthis.hydra.data.tree.ReadTreeNode;
import com.addthis.hydra.data.tree.TreeNodeData;

import com.google.common.collect.Iterators;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * For retrieval of nodes by name.
 *
 * @user-reference
 */
public class QueryElementNode implements Codec.Codable {

    private static String memKey = "";

    @Codec.Set(codable = true)
    public String match[];
    @Codec.Set(codable = true)
    public String trap[];
    @Codec.Set(codable = true)
    public String data;
    @Codec.Set(codable = true)
    public String dataKey;
    @Codec.Set(codable = true)
    public Boolean flat;
    // output column this element is bound to 'show' (or null if dropped)
    @Codec.Set(codable = true)
    private String column;
    @Codec.Set(codable = true)
    public Boolean regex;
    @Codec.Set(codable = true)
    public Boolean range;
    @Codec.Set(codable = true)
    public Boolean not;
    @Codec.Set(codable = true)
    public String path[];
    @Codec.Set(codable = true)
    public Boolean up;

    private BundleField field;
    private Pattern[] regexPatterns;

    private enum MODE {
        MATCH, TRAP
    }

    public QueryElementNode parse(String tok, MutableInt nextColumn) {
        if (tok.equals("+..")) {
            up = true;
            column = Integer.toString(nextColumn.intValue());
            nextColumn.increment();
            return this;
        }
        if (tok.equals("..")) {
            up = true;
            return this;
        }
        List<String> matchList = new ArrayList<>(3);
        List<String> trapList = new ArrayList<>(3);
        if (tok.indexOf(';') > 0) {
            tok = tok.replace(';', ',');
            flat = true;
        }
        if (tok.startsWith("!")) {
            not = true;
            regex = true;
            tok = tok.substring(1);
        }
        if (tok.startsWith("|")) {
            tok = tok.substring(1);
            regex = true;
        }

        QueryElementNode.MODE mode = MODE.MATCH;

        String list[] = Strings.splitArray(tok, ",");
        for (String component : list) {
            if (component.startsWith("*")) {
                component = component.substring(1);
                mode = MODE.MATCH;
            } else if (component.startsWith("+")) {
                component = component.substring(1);
                if (component.startsWith("+")) {
                    component = component.substring(1);
                    range = true;
                }
                mode = MODE.MATCH;
                int close;
                if (component.startsWith("{") && (close = component.indexOf("}")) > 0) {
                    column = component.substring(1, close);
                    component = component.substring(close + 1);
                } else {
                    column = Integer.toString(nextColumn.intValue());
                    nextColumn.increment();
                }
            } else if (component.startsWith("-")) {
                component = component.substring(1);
                mode = MODE.TRAP;
            }
            if (component.startsWith("%?")) {
                data = memKey;
                regex = true;
                continue;
            }
            if (component.startsWith("%") && !(component.startsWith("%2d") || component.startsWith("%2c"))) {
                String kv[] = Bytes.urldecode(component.substring(1)).split("=", 2);
                if (kv.length == 2) {
                    data = kv[0];
                    dataKey = kv[1];
                } else if (kv.length == 1) {
                    data = kv[0];
                }
                continue;
            }
            if (component.startsWith("@")) {
                path = Strings.splitArray(Bytes.urldecode(component.substring(1)), "/");
                continue;
            }
            component = Bytes.urldecode(component);
            if (component.length() > 0) {
                if (mode == MODE.MATCH) {
                    matchList.add(component);
                } else {
                    trapList.add(component);
                }
            }
        }
        if (matchList.size() > 0) {
            String out[] = new String[matchList.size()];
            match = matchList.toArray(out);
            if (tok.startsWith(",")) {
                TreeSet<String> sorted = new TreeSet<>();
                sorted.addAll(matchList);
                match = sorted.toArray(out);
            }
        }
        if (trapList.size() > 0) {
            trap = trapList.toArray(new String[trapList.size()]);
        }
        return this;
    }

    void toCompact(StringBuilder sb) {
        if (column == null && match == null) {
            sb.append("*");
        }
        if (regex()) {
            sb.append("|");
        }
        if (show()) {
            sb.append("+");
        }
        if (show() && range()) {
            sb.append("+");
        }
        if (match != null && match.length > 0) {
            int i = 0;
            for (String m : match) {
                if (i++ > 0) {
                    sb.append(",");
                }
                sb.append(Bytes.urlencode(m));
            }
        }
        if (data != null) {
            sb.append("%");
            sb.append(regex() ? "?" : data);
        }
        if (dataKey != null) {
            sb.append("=").append(Bytes.urlencode(dataKey));
        }
    }

    public boolean up() {
        return up != null && up;
    }

    public boolean flat() {
        return flat != null && flat;
    }

    public String column() {
        return column;
    }

    public boolean show() {
        return column != null;// show != null && show.booleanValue();
    }

    public BundleField field(BundleFormat format) {
        if (field == null) {
            field = format.getField(column);
        }
        return field;
    }

    public boolean regex() {
        return regex != null && regex;
    }

    public boolean range() {
        return range != null && range;
    }

    public boolean not() {
        return not != null && not;
    }

    private DataTreeNode followPath(DataTreeNode from, String path[]) {
        DataTreeNode node = from;
        for (String name : path) {
            node = node.getNode(name);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    public Iterator<DataTreeNode> getNodes(LinkedList<DataTreeNode> stack) {
        List<DataTreeNode> ret = null;
        if (up()) {
            ret = new ArrayList<>(1);
            ret.add(stack.get(1));
            return ret.iterator();
        }
        DataTreeNode parent = stack.peek();
        try {
            DataTreeNode tmp;
            if (path != null) {
                DataTreeNode refnode = followPath(parent.getTreeRoot(), path);
                return refnode != null ? new ReferencePathIterator(refnode, parent) : null;
            }
            if (trap != null) {
                for (String name : trap) {
                    for (ClosableIterator<DataTreeNode> iter = parent.getIterator(); iter.hasNext();) {
                        tmp = iter.next();
                        if (regex()) {
                            if (tmp.getName().matches(name)) {
                                iter.close();
                                return null;
                            }
                        } else {
                            if (tmp.getName().equals(name)) {
                                iter.close();
                                return null;
                            }
                        }
                    }
                }
            }
            if (match == null && regex == null && data == null) {
                return parent.getIterator();
            }
            ret = new LinkedList<>();
            if (match != null) {
                if (regex()) {
                    if (regexPatterns == null) {
                        regexPatterns = new Pattern[match.length];
                        for (int i = 0; i < match.length; i++) {
                            regexPatterns[i] = Pattern.compile(match[i]);
                        }
                    }
                    for (Iterator<DataTreeNode> iter = parent.getIterator(); iter.hasNext();) {
                        tmp = iter.next();
                        for (Pattern name : regexPatterns) {
                            if (name.matcher(tmp.getName()).matches() ^ not()) {
                                ret.add(tmp);
                            }
                        }
                    }
                } else if (range()) {
                    if (match.length == 0) {
                        return parent.getIterator();
                    } else if (match.length == 1) {
                        return parent.getIterator(match[0]);
                    } else {
                        ArrayList<Iterator<DataTreeNode>> metaIterator = new ArrayList<>();

                        for (String name : match) {
                            metaIterator.add(parent.getIterator(name));
                        }

                        return Iterators.concat(metaIterator.iterator());
                    }
                } else {
                    for (String name : match) {
                        DataTreeNode find = parent.getNode(name);
                        if (find != null) {
                            ret.add(find);
                        }
                    }
                }
            }
            if (data != null) {
                if (regex()) {
                    if (parent.getDataMap() != null) {
                        for (Map.Entry<String, TreeNodeData> actor : parent.getDataMap().entrySet()) {
                            int memSize = CodecBin2.encodeBytes(actor.getValue()).length;
                            ReadTreeNode memNode = new ReadTreeNode(actor.getKey(), memSize);
                            ret.add(memNode);
                        }
                    }
                } else {
                    DataTreeNodeActor actor = parent.getData(data);
                    if (actor != null) {
                        Collection<DataTreeNode> nodes = actor.onNodeQuery(dataKey);
                        if (nodes != null) {
                            ret.addAll(nodes);
                        }
                    }
                }
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return ret.iterator();
    }
}
