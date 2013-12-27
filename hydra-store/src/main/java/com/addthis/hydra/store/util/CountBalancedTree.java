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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import java.text.DecimalFormat;

import com.addthis.maljson.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a simple statistically balanced tree that uses node counts for left and right
 * to determine whether a tree is unbalanced. the path to root is traced either
 * on insert or delete depending on which is believed to be the lease likely to
 * alter the tree.
 * <p/>
 * rotate when: on insert: most unbalanced node highest in the tree with empty
 * leaves depth > ideal * 2
 *
 * @param <K>
 * @param <V>
 */
public final class CountBalancedTree<K extends Comparable<K>, V> implements SortedMap<K, V> {

    protected static final Logger log = LoggerFactory.getLogger(CountBalancedTree.class);
    protected static final boolean debug = false;

    protected ANode<K, V> root;
    protected int rotations;

    /**
     * check tree vitals, balance, etc
     */
    public String debug(boolean brief) {
        LevelIterator s = new LevelIterator(root);
        TreeMap<ANode<K, V>, Integer> leafs = new TreeMap<ANode<K, V>, Integer>();
        ANode<K, V> maxTilt = null;
        int scanned = 0;
        while (s.hasNext()) {
            scanned++;
            ANode<K, V> n = s.next();
            if (n.left == null || n.right == null) {
                leafs.put(n, n.left == n.right ? 2 : 1);
            }
            if (maxTilt == null || Math.abs(n.rightnodes - n.leftnodes) > Math.abs(maxTilt.rightnodes - maxTilt.leftnodes)) {
                maxTilt = n;
            }
        }
        int max = s.level();
        int idealdepth = size();
        int iter = 0;
        while (idealdepth != 0) {
            idealdepth >>= 1;
            iter++;
        }
        ANode<K, V> last = null;
        for (Iterator<ANode<K, V>> i = new AscendingIterator(first(), null); i.hasNext();) {
            ANode<K, V> n = i.next();
            if (last != null && n.compareTo(last) <= 0) throw new AssertionError(n + " <= " + last);
            last = n;
        }
        assert (check(size() == scanned, hashCode() + " size()=" + size() + " scanned=" + scanned));
        assert (check(max >= iter, hashCode() + " max " + max + " >= iter " + iter));
        String append = (size() != scanned ? " SIZE-ERROR " : "") + (brief ? "" : " json=" + toJSON());
        return ("rot=" + rotations + " size=" + size() + " scan=" + scanned + " depth=" + max + " ideal=" + iter + " leafs=" + leafs.size() + " maxTilt=" + maxTilt + append);
    }

    protected boolean check(boolean check, String msg) {
        if (!check) {
            System.out.println("*** " + msg);
        }
        return check;
    }

    public ANode<K, V> getRootNode() {
        return root;
    }

    public JSONArray toJSON() {
        return root != null ? root.toJSON() : new JSONArray();
    }

    /**
     * for debug
     */
    protected Set<ANode<K, V>> nodeSet() {
        return new AbstractSet<ANode<K, V>>() {
            @Override
            public Iterator<ANode<K, V>> iterator() {
                return new AscendingIterator(first(), null);
            }

            @Override
            public int size() {
                return CountBalancedTree.this.size();
            }
        };
    }

    /**
     * remove and return the larger branch off the root node as a separate tree
     */
    public synchronized CountBalancedTree<K, V> split() {
        CountBalancedTree<K, V> at = new CountBalancedTree<K, V>();
        if (root == null || root.left == null || root.right == null) {
            log.warn("anomalous split on " + debug(true) + " " + at.toJSON());
            if ((root.left == null || root.right == null) && size() > 2) {
                root.rotate(CountBalancedTree.this);
            } else {
                return at;
            }
        }

        ANode<K, V> left = root.left;
        ANode<K, V> right = root.right;
        left.parent = null;
        right.parent = null;
        root.left = null;
        root.right = null;
        root.leftnodes = 0;
        root.rightnodes = 0;
        ATrack<K, V> track = new ATrack<K, V>();
        insert(root.leftnodes > root.rightnodes ? right : left, root, track);
        root = left;

        if (track.maxTilt != null) {
            track.maxTilt.rotate(CountBalancedTree.this);
        }

        at.root = right;
        return at;
    }

    protected ANode<K, V> locate(final ANode<K, V> root, final K key) {
        if (root == null) {
            return null;
        }
        ANode<K, V> at = root;
        while (at != null) {
            int cmp = key.compareTo(at.key);
            if (cmp < 0) {
                at = at.left;
            } else if (cmp > 0) {
                at = at.right;
            } else {
                return at;
            }
        }
        return null;
    }

    protected ANode<K, V> first() {
        if (root == null) {
            return null;
        }
        ANode<K, V> at = root;
        while (at.left != null) {
            at = at.left;
        }
        return at;
    }

    protected ANode<K, V> last() {
        if (root == null) {
            return null;
        }
        ANode<K, V> at = root;
        while (at.right != null) {
            at = at.right;
        }
        return at;
    }

    public ANode<K, V> closest(final ANode<K, V> root, final K key, final boolean floor) {
        if (root == null) {
            return null;
        }
        ANode<K, V> closest = null;
        ANode<K, V> at = root;
        while (at != null) {
            int cmp = key.compareTo(at.key);
            if (cmp < 0) {
                if (!floor) {
                    closest = at;
                }
                if (at.left != null) {
                    at = at.left;
                } else {
                    return closest;
                }
            } else if (cmp > 0) {
                if (floor) {
                    closest = at;
                }
                if (at.right != null) {
                    at = at.right;
                } else {
                    return closest;
                }
            } else {
                return at;
            }
        }
        return null;
    }

    protected synchronized ANode<K, V> insert(final ANode<K, V> root, final ANode<K, V> node, final ATrack<K, V> track) {
        ANode<K, V> at = root;
        int cmp = 0;
        int nodetotal = node.leftnodes + node.rightnodes + 1;
        boolean rotate = false;
        while (at != null) {
            cmp = node.key.compareTo(at.key);
            if (cmp == 0) {
                at.incpath(track, -nodetotal);
                V hold = at.value;
                at.value = node.value;
                node.value = hold;
                at = node;
                break;
            } else if (cmp < 0) {
                track.level++;
                at.leftnodes += nodetotal;
                if (at.left != null) {
                    track.update(at);
                    at = at.left;
                    continue;
                } else {
                    rotate = at.right == null;
                    at.left = node;
                    node.parent = at;
                    at = null;
                    break;
                }
            } else if (cmp > 0) {
                track.level++;
                at.rightnodes += nodetotal;
                if (at.right != null) {
                    track.update(at);
                    at = at.right;
                    continue;
                } else {
                    rotate = at.left == null;
                    at.right = node;
                    node.parent = at;
                    at = null;
                    break;
                }
            }
        }
        if (root == null) {
            CountBalancedTree.this.root = node;
        }
        if (rotate && size() > 2 && track.maxTilt != null) {
            track.maxTilt.rotate(CountBalancedTree.this);
        }
        return at;
    }

    // --------------------------------------------------------------
    // collection interface methods
    // --------------------------------------------------------------

    @Override
    public void clear() {
        root = null;
        rotations = 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        if (root == null) {
            return false;
        }
        LinkedList<ANode<K, V>> queue = new LinkedList<ANode<K, V>>();
        queue.add(root);
        while (queue.size() > 0) {
            ANode<K, V> node = queue.remove();
            if (node.value.equals(value)) {
                return true;
            }
            if (node.left != null) {
                queue.add(node.left);
            }
            if (node.right != null) {
                queue.add(node.right);
            }
        }
        return false;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return new AbstractSet<Map.Entry<K, V>>() {
            @Override
            public Iterator<java.util.Map.Entry<K, V>> iterator() {
                return new Iterator<Map.Entry<K, V>>() {
                    AscendingIterator iter = new AscendingIterator(first(), null);
                    ANode<K, V> last = null;

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public java.util.Map.Entry<K, V> next() {
                        final ANode<K, V> n = iter.next();
                        last = n;
                        return new Map.Entry<K, V>() {
                            @Override
                            public K getKey() {
                                return n.key;
                            }

                            @Override
                            public V getValue() {
                                return n.value;
                            }

                            @Override
                            public V setValue(V value) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public String toString() {
                                return n.key + "=" + n.value;
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        if (last != null) {
                            last.remove(CountBalancedTree.this);
                            last = null;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                };
            }

            @Override
            public int size() {
                return CountBalancedTree.this.size();
            }
        };
    }

    @Override
    public synchronized V get(final Object key) {
        ANode<K, V> n = locate(root, (K) key);
        if (debug) debug(true);
        return n != null ? n.value : null;
    }

    @Override
    public boolean isEmpty() {
        return root == null;
    }

    @Override
    public Set<K> keySet() {
        return new AbstractSet<K>() {
            @Override
            public Iterator<K> iterator() {
                return new Iterator<K>() {
                    AscendingIterator iter = new AscendingIterator(first(), null);
                    ANode<K, V> last = null;

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public K next() {
                        last = iter.next();
                        return last.key;
                    }

                    @Override
                    public void remove() {
                        if (last != null) {
                            last.remove(CountBalancedTree.this);
                            last = null;
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    }
                };
            }

            @Override
            public int size() {
                return CountBalancedTree.this.size();
            }
        };
    }

    @Override
    public synchronized V put(final K key, final V value) {
        ANode<K, V> node = new ANode<K, V>();
        node.key = key;
        node.value = value;
        ATrack<K, V> track = new ATrack<K, V>();
        ANode<K, V> old = insert(root, node, track);
        track.clear();
        if (debug) debug(true);
        if (old != null) {
            return old.value;
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> s : m.entrySet()) {
            put(s.getKey(), s.getValue());
        }
    }

    @Override
    public synchronized V remove(final Object key) {
        if (root != null) {
            ANode<K, V> n = locate(root, (K) key);
            if (n != null) {
                n.remove(CountBalancedTree.this);
                if (debug) debug(true);
                return n.value;
            }
        }
        return null;
    }

    @Override
    public int size() {
        return root != null ? root.leftnodes + root.rightnodes + 1 : 0;
    }

    @Override
    public Collection<V> values() {
        return new AbstractCollection<V>() {
            @Override
            public Iterator<V> iterator() {
                return new Iterator<V>() {
                    AscendingIterator iter = new AscendingIterator(first(), null);
                    ANode<K, V> last = null;

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public V next() {
                        last = iter.next();
                        return last.value;
                    }

                    @Override
                    public void remove() {
                        if (last != null) {
                            last.remove(CountBalancedTree.this);
                            last = null;
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    }
                };
            }

            @Override
            public int size() {
                return CountBalancedTree.this.size();
            }
        };
    }

    // --------------------------------------------------------------
    // node class
    // --------------------------------------------------------------

    // --------------------------------------------------------------
    // helper classes
    // --------------------------------------------------------------

    /**
     * for ascending ordered set iteration. walks the tree.
     */
    private class AscendingIterator implements Iterator<ANode<K, V>> {

        protected ANode<K, V> next;
        protected ANode<K, V> limit;
        protected ANode<K, V> last;

        AscendingIterator(ANode<K, V> start, final ANode<K, V> limit) {
            this.limit = limit;
            next = fetch(start);
        }

        protected ANode<K, V> fetch(final ANode<K, V> def) {
            if (next == null) {
                return def;
            }
            ANode<K, V> n = next.next();
            return limit == null || n.compareTo(limit) < 0 ? n : null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public ANode<K, V> next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            ANode<K, V> n = next;
            next = fetch(null);
            last = n;
            return n;
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            if (last.left != null && last.right != null) {
                next = last;
            }
            last.remove(CountBalancedTree.this);
            last = null;
        }
    }

    /**
     * for descending ordered set iteration. walks the tree.
     */
    private class DescendingIterator extends AscendingIterator {

        DescendingIterator(CountBalancedTree<K, V> tree, ANode<K, V> start, ANode<K, V> limit) {
            super(start, limit);
        }

        @Override
        protected ANode<K, V> fetch(final ANode<K, V> def) {
            if (next == null) {
                return def;
            }
            ANode<K, V> n = next.previous();
            return limit == null || n.compareTo(limit) > 0 ? n : null;
        }
    }

    /**
     * for tree level iteration. walks the tree.
     */
    class LevelIterator implements Iterator<ANode<K, V>> {

        LinkedList<ANode<K, V>> current = new LinkedList<ANode<K, V>>();
        LinkedList<ANode<K, V>> next = new LinkedList<ANode<K, V>>();
        ANode<K, V> last;
        int level = 0;

        LevelIterator(final ANode<K, V> root) {
            if (root != null) {
                current.add(root);
            }
        }

        public int level() {
            return level;
        }

        @Override
        public boolean hasNext() {
            return current.size() > 0;
        }

        @Override
        public ANode<K, V> next() {
            last = current.remove();
            if (last.left != null) {
                next.add(last.left);
            }
            if (last.right != null) {
                next.add(last.right);
            }
            if (current.size() == 0) {
                level++;
                LinkedList<ANode<K, V>> chold = current;
                current = next;
                next = chold;
            }
            return last;
        }

        @Override
        public void remove() {
            if (last != null) {
                last.remove(CountBalancedTree.this);
                last = null;
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    // --------------------------------------------------------------
    // sortedmap interface methods
    // --------------------------------------------------------------

    @Override
    public Comparator<? super K> comparator() {
        return null;
    }

    @Override
    public K firstKey() {
        if (root == null) {
            return null;
        }
        ANode<K, V> at = root;
        while (at.left != null) {
            at = at.left;
        }
        return at.key;
    }

    @Override
    public SortedMap<K, V> headMap(final K toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K lastKey() {
        if (root == null) {
            return null;
        }
        ANode<K, V> at = root;
        while (at.right != null) {
            at = at.right;
        }
        return at.key;
    }

    @Override
    public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SortedMap<K, V> tailMap(final K fromKey) {
        // TODO Auto-generated method stub
        return null;
    }
}

final class ANode<K extends Comparable<K>, V> implements Comparable<ANode<K, V>> {

    private static final DecimalFormat df = new DecimalFormat("0.000");

    ANode<K, V> parent;
    ANode<K, V> left;
    ANode<K, V> right;
    K key;
    V value;
    int leftnodes;
    int rightnodes;

    public JSONArray toJSON() {
        JSONArray o = new JSONArray();
        try {
            o.put(key.toString());
            o.put(left != null ? left.toJSON() : 0);
            o.put(right != null ? right.toJSON() : 0);
            o.put("l=" + leftnodes + ",r=" + rightnodes + ",t=" + df.format(tilt()));
        } catch (Exception ex) {
        }
        return o;
    }

    protected ANode<K, V> next() {
        if (right != null) {
            ANode<K, V> n = right;
            while (n.left != null) {
                n = n.left;
            }
            return n;
        } else {
            ANode<K, V> p = parent;
            ANode<K, V> n = this;
            while (p != null && n == p.right) {
                n = p;
                p = p.parent;
            }
            return p;
        }
    }

    protected ANode<K, V> previous() {
        if (left != null) {
            ANode<K, V> n = left;
            while (n.right != null) {
                n = n.right;
            }
            return n;
        } else {
            ANode<K, V> p = parent;
            ANode<K, V> n = this;
            while (p != null && n == p.left) {
                n = p;
                p = p.parent;
            }
            return p;
        }
    }

    protected void updateParentChild(final CountBalancedTree<K, V> ctx, final ANode<K, V> newnode) {
        if (parent != null) {
            if (parent.left == this) {
                parent.left = newnode;
            } else {
                parent.right = newnode;
            }
        } else {
            ctx.root = newnode;
        }
        if (newnode != null) {
            newnode.parent = parent;
        }
    }

    protected void incpath(final ATrack<K, V> track, final int value) {
        ANode<K, V> last = this;
        ANode<K, V> up = this.parent;
        while (up != null) {
            if (up.left == last) {
                up.leftnodes += value;
            } else {
                up.rightnodes += value;
            }
            track.update(up);
            last = up;
            up = up.parent;
        }
    }

    protected synchronized void remove(final CountBalancedTree<K, V> ctx) {
        ATrack<K, V> track = new ATrack<K, V>();
        incpath(track, -1);
        if (left == right) {
            updateParentChild(ctx, null);
        } else if (leftnodes < rightnodes) {
            updateParentChild(ctx, right);
            if (left != null) {
                ctx.insert(right, left, track);
            }
        } else {
            updateParentChild(ctx, left);
            if (right != null) {
                ctx.insert(left, right, track);
            }
        }
        track.clear();
        enablegc();
    }

    protected void rotate(final CountBalancedTree<K, V> ctx) {
        if (rightnodes + leftnodes == 0) {
            return;
        }
        try {
            ctx.rotations++;
            // pivot left b/c imbalance is to the right
            if (rightnodes > leftnodes) {
                updateParentChild(ctx, right);
                parent = right;
                right = parent.left;
                rightnodes = 0;
                if (right != null) {
                    rightnodes += (right.rightnodes + right.leftnodes + 1);
                    right.parent = this;
                }
                parent.left = this;
                parent.leftnodes = (rightnodes + leftnodes + 1);
            } else { // pivot right b/c imbalance is to the left
                updateParentChild(ctx, left);
                parent = left;
                left = parent.right;
                leftnodes = 0;
                if (left != null) {
                    leftnodes += (left.rightnodes + left.leftnodes + 1);
                    left.parent = this;
                }
                parent.right = this;
                parent.rightnodes = (rightnodes + leftnodes + 1);
            }
            if (ctx.root == this) {
                ctx.root = parent;
            }
        } catch (RuntimeException ex) {
            ctx.log.warn("can't rotate " + this + " b/c " + ex);
            throw ex;
        }
    }

    void sanityAll() {
        sanityCheck();
        if (parent != null) {
            parent.sanityCheck();
        }
        if (right != null) {
            right.sanityCheck();
        }
        if (left != null) {
            left.sanityCheck();
        }
    }

    void sanityCheck() {
        if (leftnodes != 0 && left == null) {
            throw new RuntimeException("!!!!! left count on null node " + this);
        }
        if (rightnodes != 0 && right == null) {
            throw new RuntimeException("!!!!! right count on null node " + this);
        }
        if (leftnodes < 0 || rightnodes < 0) {
            throw new RuntimeException("!!!!! node count error " + this);
        }
        if (parent != null && parent.left != this && parent.right != this) {
            throw new RuntimeException("!!!!! i am not a child of my parent :: " + this + " parent = " + parent);
        }
        if (left != null && left.parent != this) {
            throw new RuntimeException("!!!!! left's parent is not me :: " + this + " it is " + left.parent);
        }
        if (right != null && right.parent != this) {
            throw new RuntimeException("!!!!! right's parent is not me :: " + this + " it is " + right.parent);
        }
    }

    void enablegc() {
        parent = null;
        left = null;
        right = null;
    }

    double tilt() {
        return Math.abs(leftnodes - rightnodes) / (leftnodes + rightnodes + 1d);
    }

    @Override
    public int compareTo(final ANode<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return "[" + key + "=" + value + ",ln=" + leftnodes + ",rn=" + rightnodes + ",p=" + (parent == null ? "" : parent.key) + ",l=" + (left == null ? "" : left.key) + ",r=" +
            (right == null ? "" : right.key) + "]";
    }
}

final class ATrack<K extends Comparable<K>, V> {

    int level;
    ANode<K, V> maxTilt;
    double tiltValue;

    void update(final ANode<K, V> check) {
        double tilt = check.tilt();
        if (maxTilt == null || tilt > tiltValue) {
            maxTilt = check;
            tiltValue = tilt;
        }
    }

    void clear() {
        maxTilt = null;
    }

    public String toString() {
        return "lvl=" + level + ",tilt=" + maxTilt;
    }
}

