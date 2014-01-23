package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNode;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.hydra.store.util.SeenFilterBasic;

public final class DataKeySieve2 extends TreeNodeData<DataKeySieve2.Config> implements Codec.SuperCodable {

    private final static int targetSaturation = Integer.parseInt(System.getProperty("datakeysieve2.saturation", "20"));

    /**
     * This data attachment <span class="hydra-summary">keeps a sieve of encountered values</span>.
     * <p/>
     * <p>The sieve of encountered values is represented as a stack of bloom filters.
     * If any of the bloom filters in the current stack exceeds the target saturation value then
     * a new stack of bloom filters is created. The data attachment stores a list of
     * stacks of bloom filters because we cannot transfer elements from
     * the old stack of bloom filters to the new stack of bloom filters.</p>
     * <p/>
     * <p>Each stack of bloom filters is referred to as a single layer of bloom filters.
     * The number of bloom filters within each layer is equal to the number of bloom filters
     * specified in the {@link #tiers} field.</p>
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     *   {type : "const", value : "0", data : {
     *     sieve : {type : "key.sieve2", key : "TERMS", saturation : 25, tiers : [
     *       {bits : 4000000, bitsper : 4, hash : 4},
     *       {bits : 1000000, bitsper : 4, hash : 4},
     *       {bits : 250000, bitsper : 4, hash : 4},
     *       {bits : 125000, bitsper : 4, hash : 4},
     *     ]},
     *   }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>"$" operations support the following commands in the format
     * $+{attachment}={command} :
     * <table>
     * <tr>
     * <td>layers</td>
     * <td>number of layers in the data attachment</td>
     * </tr>
     * <tr>
     * <td>satmax</td>
     * <td>maximum saturation across all layers</td>
     * </tr>
     * <tr>
     * <td>ram</td>
     * <td>number of bytes used by the data attachment</td>
     * </tr>
     * </table>
     *
     * <p>"%" operations support the following commands in the format /+%{attachment}={command}.
     *
     * <table>
     * <tr>
     * <td>"name1,name2,name3"</td>
     * <td>create virtual nodes using the keys specified in the command</td>
     * </tr>
     * </table>
     *
     * <p>Using "%" without any arguments returns one node per bloom filter
     * or N * M virtual nodes when there are N layers and
     * M bloom filters inside each layer.</p>
     *
     * @user-reference
     * @hydra-name key.sieve2
     */
    public final static class Config extends TreeDataParameters<DataKeySieve2> {

        /**
         * Bundle field name from which to draw values.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String key;

        /**
         * Stack of initial bloom filters.
         * The filters are specified from the lowest level
         * to the highest level.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private SeenFilterBasic<String> tiers[];

        /**
         * Default is either System property "datakeysieve2.saturation" or 20.
         */
        @Codec.Set(codable = true)
        private int saturation = targetSaturation;

        @Override
        public DataKeySieve2 newInstance() {
            DataKeySieve2 top = new DataKeySieve2();
            top.layers = new ArrayList<>();
            top.saturation = saturation;
            top.template = tiers;
            top.addLayer();
            return top;
        }
    }

    @Codec.Set(codable = true, required = true)
    private ArrayList<Sieve> layers;
    @Codec.Set(codable = true)
    private int saturation;

    private SeenFilterBasic<String> template[];
    private BundleField keyAccess;
    private Sieve current;

    private void addLayer() {
        int newbits[] = new int[template.length];
        if (current != null) {
            for (int idx = 0; idx < current.tiers.length; idx++) {
                SeenFilterBasic<String> filter = current.tiers[idx];
                if (filter.getSaturation() >= saturation) {
                    newbits[idx] = (int) (filter.getBits() * 1.5);
                } else {
                    double filtsat = filter.getSaturation() * 1.0d;
                    double minsat = Math.max(saturation * 0.75d, filtsat * 1.0d);
                    newbits[idx] = (int) ((minsat / saturation) * filter.getBits());
                }
            }
        }
        SeenFilterBasic<String> tmp[] = new SeenFilterBasic[template.length];
        for (int i = 0; i < template.length; i++) {
            tmp[i] = template[i].newInstance(newbits[i] > 32 ? newbits[i] : template[i].getBits());
        }
        current = new Sieve(tmp);
        layers.add(current);
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, Config conf) {
        Bundle bundle = state.getBundle();
        if (keyAccess == null) {
            keyAccess = bundle.getFormat().getField(conf.key);
        }
        return updateCounter(bundle, bundle.getValue(keyAccess));
    }

    private boolean updateCounter(Bundle bundle, ValueObject value) {
        boolean mod = false;
        if (value == null) {
            return false;
        }
        switch (value.getObjectType()) {
            case INT:
            case FLOAT:
            case STRING:
            case BYTES:
            case CUSTOM:
                String val = ValueUtil.asNativeString(value);
                if (val != null && current.updateSeen(val)) {
                    bundle.setValue(keyAccess, null);
                    if (current.isSaturated(saturation)) {
                        addLayer();
                    }
                    mod = true;
                }
                break;
            case ARRAY:
                ValueArray arr = value.asArray();
                for (ValueObject o : arr) {
                    // use "|" to prevent short circuiting
                    mod = mod | updateCounter(bundle, o);
                }
                break;
            case MAP:
                ValueMap map = value.asMap();
                for (ValueMapEntry o : map) {
                    // use "|" to prevent short circuiting
                    mod = mod | updateCounter(bundle, ValueFactory.create(o.getKey()));
                }
                break;
            default:
                throw new IllegalStateException("Unhandled object type " + value.getObjectType());
        }
        return mod;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key != null) {
            if (key.equals("layers")) {
                return ValueFactory.create(layers.size());
            }
            if (key.equals("satmax")) {
                int sat = 0;
                for (Sieve sieve : layers) {
                    sat = Math.max(sat, sieve.getSaturationMax());
                }
                return ValueFactory.create(sat);
            }
            if (key.equals("ram")) {
                int sum = 0;
                for (Sieve sieve : layers) {
                    sum += sieve.getByteSize();
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
        return Arrays.asList(new String[]{"layers", "satmax", "ram"});
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        if (key == null || key.length() == 0) {
            ArrayList<DataTreeNode> list = new ArrayList<>(layers.size() * template.length);
            for (int i = 0; i < layers.size(); i++) {
                Sieve s = layers.get(i);
                for (int j = 0; j < template.length; j++) {
                    list.add(new MyTreeNode(i + "-" + j + "-" + s.updates, s.tiers[j].getSaturation(), s.tiers[j].getBits()));
                }
            }
            return list;
        }
        String keys[] = Strings.splitArray(key, ",");
        ArrayList<DataTreeNode> list = new ArrayList<>(keys.length);
        synchronized (this) {
            for (String k : keys) {
                int count = 0;
                long hash[] = current.tiers[0].getHashSet(k);
                boolean lookDeep = false;
                for (Sieve s : layers) {
                    lookDeep = false;
                    int seen = s.getSeenLevel(hash);
                    if (seen < 0) {
                        lookDeep = true;
                    } else if (seen > 0) {
                        count += seen;
                    }
                }
                if (count > 0 || lookDeep) {
                    DataTreeNode n = lookDeep ? parent.getNode(k) : null;
                    if (n != null) {
                        list.add(new MyTreeNode(k, count + n.getCounter()));
                    } else {
                        list.add(new MyTreeNode(k, count));
                    }
                }
            }
        }
        return list.size() > 0 ? list : null;
    }

    /**
     * phantom node created for reporting
     */
    private static class MyTreeNode extends TreeNode {

        MyTreeNode(String name, long hits) {
            this.name = name;
            this.hits = hits;
        }

        MyTreeNode(String name, long hits, int nodes) {
            this.name = name;
            this.hits = hits;
            this.nodes = nodes;
        }
    }

    /**
     * for stacking
     */
    public final static class Sieve {

        @Codec.Set(codable = true, required = true)
        private SeenFilterBasic<String> tiers[];
        @Codec.Set(codable = true, required = true)
        private int updates;

        public Sieve() {
        }

        public Sieve(SeenFilterBasic<String> tiers[]) {
            this.tiers = tiers;
        }

        public boolean isSaturated(int saturation) {
            return updates++ % 100 == 0 && getSaturationMax() > saturation;
        }

        public int getByteSize() {
            int sum = 0;
            for (SeenFilterBasic<String> bloom : tiers) {
                sum += bloom.getBits() / 32;
            }
            return sum;
        }

        public int getSaturationMax() {
            int sat = 0;
            for (SeenFilterBasic<String> bloom : tiers) {
                sat = Math.max(sat, bloom.getSaturation());
            }
            return sat;
        }

        /**
         * @return true if handled in level filters
         */
        public boolean updateSeen(String k) {
            long hash[] = tiers[0].getHashSet(k);
            for (SeenFilterBasic<String> bloom : tiers) {
                if (!bloom.checkHashSet(hash)) {
                    bloom.setHashSet(hash);
                    return true;
                }
            }
            return false;
        }

        /**
         * 0 = not seen in first level (by extension any) n = was seen last at
         * level -n = seen in every level
         */
        public int getSeenLevel(long hash[]) {
            int count = 0;
            for (SeenFilterBasic<String> bloom : tiers) {
                if (!bloom.checkHashSet(hash)) {
                    if (count == 0) {
                        return 0;
                    }
                    return count;
                }
                count++;
            }
            return -count;
        }
    }

    @Override
    public void postDecode() {
        current = layers.get(layers.size() - 1);
        template = current.tiers;
    }

    @Override
    public void preEncode() {
    }
}
