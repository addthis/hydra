package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.ConcurrentTreeNode;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.ReadNode;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.data.tree.TreeNodeDataDeferredOperation;
import com.addthis.hydra.store.util.SeenFilterBasic;

/**
 * keeps a record of the top N key values and the number of times they're
 * encountered
 */
@Deprecated
public class DataKeySieve extends TreeNodeData<DataKeySieve.Config> {

    /**
     * @user-reference
     * @hydra-name key.sieve
     */
    public static final class Config extends TreeDataParameters {

        @Codec.Set(codable = true, required = true)
        private String key;
        @Codec.Set(codable = true, required = true)
        private SeenFilterBasic<String>[] tiers;
        @Codec.Set(codable = true)
        private int maxCount;

        private DataCounting.Config template;

        @Override
        public DataKeySieve newInstance() {
            DataKeySieve top = new DataKeySieve();
            top.tiers = new SeenFilterBasic[tiers.length];
            for (int i = 0; i < tiers.length; i++) {
                top.tiers[i] = tiers[i].newInstance();
            }
            if (maxCount > 0) {
                if (template == null) {
                    template = new DataCounting.Config();
                    template.max = maxCount;
                    template.key = key;
                }
                top.template = template;
                top.counts = new DataCounting[tiers.length];
                for (int i = 0; i < tiers.length; i++) {
                    top.counts[i] = template.newInstance();
                }
            }
            return top;
        }
    }

    @Codec.Set(codable = true, required = true)
    private SeenFilterBasic<String>[] tiers;
    @Codec.Set(codable = true)
    private DataCounting[] counts;

    private BundleField keyAccess;
    private DataCounting.Config template;

    @Override
    public boolean updateParentNewChild(DataTreeNodeUpdater state, DataTreeNode parentNode,
            DataTreeNode childNode,
            List<TreeNodeDataDeferredOperation> deferredOps) {
        childNode.incrementCounter(tiers.length);
        return true;
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        if (template == null && conf.maxCount > 0) {
            template = new DataCounting.Config();
            template.max = conf.maxCount;
            template.key = conf.key;
        }
        String val = ValueUtil.asNativeString(p.getValue(keyAccess));
        if (val != null) {
            int count = 0;
            for (SeenFilterBasic<String> bloom : tiers) {
                if (!bloom.getSeen(val)) {
                    bloom.setSeen(val);
                    p.setValue(keyAccess, null);
                    if (counts != null) {
                        counts[count].offer(val);
                    }
                    return true;
                }
                count++;
            }
        }
        return false;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key != null) {
            if (key.equals("satmax")) {
                int sat = 0;
                for (SeenFilterBasic<String> bloom : tiers) {
                    sat = Math.max(sat, bloom.getSaturation());
                }
                return ValueFactory.create(sat);
            }
            if (key.equals("satmin")) {
                int sat = 0;
                for (SeenFilterBasic<String> bloom : tiers) {
                    int bs = bloom.getSaturation();
                    sat = sat == 0 ? bs : Math.max(sat, bs);
                }
                return ValueFactory.create(sat);
            }
            if (key.equals("satavg")) {
                int sum = 0;
                for (SeenFilterBasic<String> bloom : tiers) {
                    sum += bloom.getSaturation();
                }
                return ValueFactory.create(sum / tiers.length);
            }
            if (key.equals("ram")) {
                int sum = 0;
                for (SeenFilterBasic<String> bloom : tiers) {
                    sum += bloom.getBits() / 32;
                }
                return ValueFactory.create(sum);
            }
        }
        return null;
    }

    /**
     * return types of synthetic nodes returned
     */
    public List<String> getNodeTypes() {
        return Arrays.asList("sat");
    }

    /**
     * query node/%token=% to get the histogram for calculating entropy
     */
    @Override
    public Collection<ReadNode> getNodes(ReadNode parent, String key) {
        if (key == null || key.length() == 0) {
            ArrayList<ReadNode> list = new ArrayList<>(tiers.length);
            for (SeenFilterBasic<String> bloom : tiers) {
                list.add(new MyTreeNode(Integer.toHexString(bloom.hashCode()), bloom.getSaturation(), bloom.getBits()));
            }
            return list;
        }
        // for generating entropy
        if (key.equals("%") && counts != null) {
            ArrayList<ReadNode> list = new ArrayList<>(tiers.length);
            long thits = 0;
            TreeMap<Long, MyTreeNode> inst = new TreeMap<>();
            for (Iterator<? extends ReadNode> iter = parent.getIterator(); iter.hasNext(); ) {
                ReadNode n = iter.next();
                long l = n.getCounter();
                MyTreeNode v = inst.get(l);
                if (v == null) {
                    v = new MyTreeNode(Long.valueOf(n.getCounter()).toString(), 0, 0);
                }
                v.incrementCounter(n.getCounter());
                v.decNodes();
                thits += n.getCounter();
                inst.put(l, v);
            }
            // TODO fix dependency on static reference to hydra.Config -- need
            // to pass a PacketContext on call
            DataCounting merge = template.newInstance();
            for (int i = tiers.length; i > 0; i--) {
                long oldcount = merge.count();
                merge.merge(counts[i - 1]);
                long diffcount = merge.count() - oldcount;
                inst.put((long) i, new MyTreeNode(Long.valueOf(i).toString(), diffcount * i, (int) diffcount));
            }
            for (Entry<Long, MyTreeNode> s : inst.entrySet()) {
                list.add(new MyTreeNode(s.getKey().toString(), s.getValue().getCounter(), s.getValue().getNodeCount()));
            }
            return list;
        }
        String[] keys = Strings.splitArray(key, ",");
        ArrayList<ReadNode> list = new ArrayList<>(keys.length);
        synchronized (this) {
            keyloop:
            for (String k : keys) {
                int count = 0;
                for (SeenFilterBasic<String> bloom : tiers) {
                    if (!bloom.getSeen(k)) {
                        if (count == 0) {
                            continue keyloop;
                        }
                        list.add(new MyTreeNode(k, counts == null ? count : counts[count].count()));
                        continue keyloop;
                    }
                    count++;
                }
                ReadNode n = parent.getNode(k);
                if (n != null) {
                    list.add(n);
                } else {
                    list.add(new MyTreeNode(k, counts == null ? count : counts[count].count()));
                }
            }
        }
        return list.size() > 0 ? list : null;
    }

    /**
     * phantom node created for reporting
     */
    private static class MyTreeNode extends ConcurrentTreeNode {

        MyTreeNode(String name, long hits) {
            this.name = name;
            this.hits = hits;
        }

        MyTreeNode(String name, long hits, int nodes) {
            this.name = name;
            this.hits = hits;
            this.nodes = nodes;
        }

        void decNodes() {
            nodes--;
        }
    }
}
