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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.addthis.basis.util.ClosableIterator;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.TreeNode;

import org.apache.commons.lang3.mutable.MutableInt;


public class QueryElement implements Codec.SuperCodable {

    private static final boolean debug = System.getProperty("query.path.debug", "0").equals("1");

    /**
     *
     */
    public QueryElement() {
    }

    /**
     * Node names to iterate over.
     */
    @Codec.Set(codable = true)
    private QueryElementNode node;

    /**
     * For each node the field values to collect.
     */
    @Codec.Set(codable = true)
    private ArrayList<QueryElementField> field;

    /**
     * For each node the property values to collect.
     */
    @Codec.Set(codable = true)
    private ArrayList<QueryElementProperty> prop;

    /**
     * If true then do not fail when a node or property is missing. Default is false.
     */
    @Codec.Set(codable = true)
    private Boolean nullok;

    /**
     * Skip the first N nodes for this element.
     */
    @Codec.Set(codable = true)
    private Integer skip;

    /**
     * Limit the total number of nodes for this element.
     */
    @Codec.Set(codable = true)
    private Integer limit;

    // internal state
    private boolean hasdata;

    /**
     * simplified compact, not complete
     */
    public String toCompact(StringBuilder sb) {
        if (emptyok()) {
            sb.append("~");
        }
        if (limit() > 0 || skip() > 0) {
            sb.append("(" + (skip == null ? 0 : skip) + "-" + limit + ")");
        }
        node.toCompact(sb);
        if (prop != null && prop.size() > 0) {
            sb.append(":");
            int i = 0;
            for (QueryElementProperty p : prop) {
                if (i++ > 0) {
                    sb.append(",");
                }
                p.toCompact(sb);
            }
        }
        if (field != null && field.size() > 0) {
            sb.append("$");
            int i = 0;
            for (QueryElementField f : field) {
                if (i++ > 0) {
                    sb.append(",");
                }
                f.toCompact(sb);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this);
        } catch (Exception e) {
            return super.toString();
        }
    }

    /**
     * element parser
     */
    public QueryElement parse(String q, MutableInt nextColumn) {
        int pos = 0;
        if (q.startsWith("(") && (pos = q.indexOf(")")) > 0) {
            String range[] = Strings.splitArray(q.substring(1, pos), "-");
            if (range.length == 1) {
                limit = Integer.parseInt(range[0]);
            } else {
                skip = Integer.parseInt(range[0]);
                limit = Integer.parseInt(range[1]);
            }
            q = q.substring(pos + 1);
        }
        if (q.startsWith("~")) {
            q = q.substring(1);
            nullok = true;
        }
        StringTokenizer st = new StringTokenizer(q, ":$", true);
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();
            node = new QueryElementNode().parse(tok, nextColumn);
            while (st.hasMoreTokens()) {
                String sep = st.nextToken();
                if (st.hasMoreTokens()) {
                    tok = st.nextToken();
                    if (sep.equals(":")) {
                        String ps[] = Strings.splitArray(tok, ",");
                        if (prop == null) {
                            prop = new ArrayList<>(ps.length);
                        }
                        for (String p : ps) {
                            prop.add(new QueryElementProperty().parse(p, nextColumn));
                        }
                    } else if (sep.equals("$")) {
                        if (field == null) {
                            field = new ArrayList<>(1);
                        }
                        field.add(new QueryElementField().parse(tok, nextColumn));
                    }
                }
            }
        }
        if (debug) {
            try {
                System.out.println("new QE " + CodecJSON.encodeString(this));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        postDecode();
        return this;
    }

    public int skip() {
        return skip != null ? skip : 0;
    }

    public int limit() {
        return limit != null ? limit : 0;
    }

    public boolean flatten() {
        return node.flat();
    }

    public boolean emptyok() {
        return nullok != null && nullok;
    }

    public boolean hasData() {
        return hasdata;
    }

    public Iterator<DataTreeNode> matchNodes(DataTree tree, LinkedList<DataTreeNode> stack) {
        return node != null ? node.getNodes(stack) : null;
    }

    public int update(FieldValueList fvlist, DataTreeNode tn) {
        if (tn == null) {
            return 0;
        }
        int updates = 0;
        if (node != null && node.show()) {
            fvlist.push(node.field(fvlist.getFormat()), ValueFactory.create(tn.getName()));
            updates++;
        }
        if (prop != null) {
            for (QueryElementProperty p : prop) {
                ValueObject val = p.getValue(tn);
                if (val == null && !emptyok()) {
                    fvlist.rollback();
                    return 0;
                }
                if (p.show()) {
                    fvlist.push(p.field(fvlist.getFormat()), val);
                    updates++;
                }
            }
        }
        if (field != null) {
            for (QueryElementField f : field) {
                for (ValueObject val : f.getValues(tn)) {
                    if (val == null && !emptyok()) {
                        fvlist.rollback();
                        return 0;
                    }
                    if (f.show()) {
                        fvlist.push(f.field(fvlist.getFormat()), val);
                        updates++;
                    }
                }
            }
        }
        fvlist.commit();
        return updates;
    }

    /**
     * phantom node created for reporting
     */
    private static class PhantomNode extends TreeNode {

        PhantomNode(String name, long hits, int nodes) {
            this.name = name;
            this.hits = hits;
            this.nodes = nodes;
        }
    }

    // preset 'hasdata' instead of computing it each time
    @Override
    public void postDecode() {
        if (node != null) {
            hasdata = node.show();
        }
        if (field != null) {
            for (QueryElementField f : field) {
                hasdata |= f.show();
            }
        }
        if (prop != null) {
            for (QueryElementProperty p : prop) {
                hasdata |= p.show();
            }
        }
    }

    @Override
    public void preEncode() {
    }

    /**
     * for node iteration using a reference node's keys
     */
    static final class ReferencePathIterator implements ClosableIterator<DataTreeNode> {

        private final ClosableIterator<DataTreeNode> refiter;
        private final DataTreeNode node;
        private DataTreeNode next;

        ReferencePathIterator(DataTreeNode refnode, DataTreeNode node) {
            this.refiter = refnode.getIterator();
            this.node = node;
        }

        @Override
        public void close() {
            refiter.close();
        }

        @Override
        public boolean hasNext() {
            while (next == null && refiter.hasNext()) {
                DataTreeNode nextRef = refiter.next();
                next = node.getNode(nextRef.getName());
            }
            return next != null;
        }

        @Override
        public DataTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DataTreeNode ret = next;
            next = null;
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public QueryElementNode getNode() {
        return node;
    }

    public ArrayList<QueryElementProperty> getProp() {
        return prop;
    }
}
