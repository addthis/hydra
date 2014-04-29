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
package com.addthis.hydra.data.tree.prop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.math.RoundingMode;

import com.addthis.basis.util.Varint;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class DataReservoir extends TreeNodeData<DataReservoir.Config> implements Codec.BytesCodable {

    private static final Logger log = LoggerFactory.getLogger(DataReservoir.class);

    private static final ImmutableList<DataTreeNode> EMPTY_LIST = ImmutableList.<DataTreeNode>builder().build();

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * This data attachment <span class="hydra-summary">keeps circular buffer
     * of N counters</span>.
     *
     * <p>The numbers of buckets that are stored is determined by the {@link #size}
     * parameter. The value stored in the {@link #epochField} bundle field determines
     * the current epoch. The counter stored within this epoch is incremented. Older
     * epochs are dropped as newer epochs are encountered.
     *
     * <p>The data attachment is queried with the notation {@code /+%name=epoch=N~sigma=N.N~obs=N}.
     * Epoch determines the epoch to be tested. sigma is the number of standard deviations
     * to use as a threshold. obs specifies how many previous observations to use. All these
     * fields are required. Specifying min=N is an optional parameter for a minimum number
     * of observations that must be detected. The output returned is of the form
     * {@code /delta:+hits/measurement:+hits/mean:+hits/stddev:+hits/threshold:+hits}.
     *
     * @user-reference
     * @hydra-name reservoir
     */
    public static final class Config extends TreeDataParameters<DataReservoir> {

        /**
         * Bundle field name from which to draw the epoch.
         * This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String epochField;

        /**
         * Size of the reservoir. This field is required.
         * @return
         */
        @Codec.Set(codable = true, required = true)
        private int size;

        @Override
        public DataReservoir newInstance() {
            DataReservoir reservoir = new DataReservoir();
            return reservoir;
        }
    }

    @Codec.Set(codable = true, required = true)
    private int[] reservoir;

    /**
     * The minEpoch is a monotonically increasing value.
     * An increase in this value is associated with the elimination
     * of state from older epochs. All effort is made to increment the
     * value as little as possible.
     */
    @Codec.Set(codable = true, required = true)
    private long minEpoch;

    private BundleField keyAccess;

    /**
     * Resize the reservoir to the new size.
     * If the reservoir has not yet been allocated then
     * construct it. If the requested length is smaller
     * than the reservoir then discard the oldest values.
     * If the requested length is larger than the reservoir
     * then allocate additional space for the reservoir.
     *
     * @param newsize
     */
    private void resize(int newsize) {
        if (reservoir == null) {
            reservoir = new int[newsize];
        } else if (reservoir.length < newsize) {
            int[] newReservoir = new int[newsize];
            System.arraycopy(reservoir, 0, newReservoir, 0, reservoir.length);
            reservoir = newReservoir;
        } else if (reservoir.length > newsize) {
            int[] newReservoir = new int[newsize];
            System.arraycopy(reservoir, reservoir.length - newsize, newReservoir, 0, newsize);
            minEpoch += (reservoir.length - newsize);
            reservoir = newReservoir;
        }
    }

    /**
     * Shift the minimum epoch to accommodate
     * the new epoch. If the epoch is less than the minimum
     * epoch then do nothing. If the epoch falls within the boundary
     * of the the reservoir then do nothing. If the epoch is farther
     * away then one length away from the maximum epoch, then empty
     * out the reservoir and set the maximum epoch to the target
     * epoch. Otherwise shift the reservoir to accommodate the new
     * epoch.
     *
     * @param epoch
     */
    private void shift(long epoch) {
        long delta = (epoch - minEpoch);
        if (delta < reservoir.length) {
            return;
        } else if (delta > 2 * (reservoir.length - 1)) {
            Arrays.fill(reservoir, 0);
            minEpoch = epoch - (reservoir.length - 1);
        } else {
            int shift = (int) (delta - reservoir.length + 1);
            System.arraycopy(reservoir, shift, reservoir, 0, reservoir.length - shift);
            Arrays.fill(reservoir, reservoir.length - shift, reservoir.length, 0);
            minEpoch += shift;
        }
    }

    /**
     * Insert the new epoch. Assumes that {@link #shift(long epoch)}
     * has previously been invoked.
     *
     * @param epoch
     */
    private void update(long epoch, long count) {
        if (epoch < minEpoch) {
            return;
        } else {
            reservoir[(int) (epoch - minEpoch)] += count;
        }
    }

    /**
     * Update the reservoir with the input epoch and a value of one.
     *
     * @param epoch input time period
     * @param size alters the capacity of the reservoir
     */
    @VisibleForTesting
    void updateReservoir(long epoch, int size) {
        updateReservoir(epoch, size, 1);
    }

    /**
     * Update the reservoir with the input epoch and specified additional count.
     *
     * @param epoch input time period
     * @param size alters the capacity of the reservoir
     * @param count amount to increment the time period
     */
    @VisibleForTesting
    void updateReservoir(long epoch, int size, long count) {
        resize(size);
        shift(epoch);
        update(epoch, count);
    }

    /**
     * Return the count associated with the input epoch,
     * or an error value if the input is out of bounds.
     *
     * @param epoch
     * @return the non-negative count or -1 if input is less
     *         than minimum epoch or -2 if input is greater
     *         than maximum epoch or -3 if the data structure
     *         has not been initialized.
     */
    @VisibleForTesting
    int retrieveCount(long epoch) {
        if (reservoir == null) {
            return -3;
        } else if (epoch < minEpoch) {
            return -1;
        } else if (epoch >= (minEpoch + reservoir.length)) {
            return -2;
        } else {
            return reservoir[(int) (epoch - minEpoch)];
        }
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state,
            DataTreeNode childNode, DataReservoir.Config conf) {
        if (keyAccess == null) {
            keyAccess = state.getBundle().getFormat().getField(conf.epochField);
        }
        ValueObject val = state.getBundle().getValue(keyAccess);
        if (val != null) {
            try {
                long epoch = val.asLong().getLong();
                updateReservoir(epoch, conf.size);
                return true;
            } catch (Exception ex) {
                log.error("Error trying to insert " + val + " into reservoir: ", ex);
            }
        }
        return false;
    }

    @Override
    public ValueObject getValue(String key) {
        return null;
    }

    /**
     * Helper method for {@link #getNodes(com.addthis.hydra.data.tree.DataTreeNode, String)}
     * If raw=true then add nodes for the raw observations.
     */
    private void addRawObservations(List<DataTreeNode> result) {
        result.add(new VirtualTreeNode("minEpoch", minEpoch));
        VirtualTreeNode[] children = new VirtualTreeNode[reservoir.length];
        for(int i = 0 ; i < reservoir.length; i++) {
            children[i] = new VirtualTreeNode(Long.toString(minEpoch + i),
                    reservoir[i]);
        }
        result.add(new VirtualTreeNode("observations", 1, children));
    }

    /**
     * Either generate some nodes for debugging purposes or
     * return an empty list.
     *
     * @param raw if true then generate debugging nodes
     * @return list of nodes
     */
    private List<DataTreeNode> makeDefaultNodes(boolean raw) {
        if (raw) {
            List<DataTreeNode> result = new ArrayList<>();
            addRawObservations(result);
            return result;
        } else {
            return EMPTY_LIST;
        }
    }

    /**
     * Convenience method to convert an node into an array of size one.
     */
    private static VirtualTreeNode[] generateSingletonArray(VirtualTreeNode value) {
        VirtualTreeNode[] result = new VirtualTreeNode[1];
        result[0] = value;
        return result;
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        long targetEpoch = -1;
        int numObservations = -1;
        double sigma = Double.POSITIVE_INFINITY;
        int measurement;
        int minMeasurement = Integer.MIN_VALUE;
        boolean raw = false;
        if (key == null) {
            return null;
        }
        String[] kvpairs = key.split("~");
        for(String kvpair : kvpairs) {
            String[] kv = kvpair.split("=");
            if (kv.length == 2) {
                String kvkey = kv[0];
                String kvvalue = kv[1];
                switch (kvkey) {
                    case "epoch":
                        targetEpoch = Long.parseLong(kvvalue);
                        break;
                    case "sigma":
                        sigma = Double.parseDouble(kvvalue);
                        break;
                    case "min":
                        minMeasurement = Integer.parseInt(kvvalue);
                        break;
                    case "obs":
                        numObservations = Integer.parseInt(kvvalue);
                        break;
                    case "raw":
                        raw = Boolean.parseBoolean(kvvalue);
                        break;
                }
            }
        }
        if (targetEpoch == -1) {
            return makeDefaultNodes(raw);
        } else if (sigma == Double.POSITIVE_INFINITY) {
            return makeDefaultNodes(raw);
        } else if (numObservations == -1) {
            return makeDefaultNodes(raw);
        } else if (reservoir == null) {
            return makeDefaultNodes(raw);
        } else if (targetEpoch < minEpoch) {
            return makeDefaultNodes(raw);
        } else if (targetEpoch >= minEpoch + reservoir.length) {
            return makeDefaultNodes(raw);
        }

        int count = 0;
        double mean = 0.0;
        double m2 = 0.0;
        double stddev;

        int index = reservoir.length - 1;
        long currentEpoch = minEpoch + index;

        while (currentEpoch != targetEpoch) {
            index--;
            currentEpoch--;
        }

        measurement = reservoir[index--];

        while (count < numObservations && index >= 0) {
            int value = reservoir[index--];
            count++;
            double delta = value - mean;
            mean += delta / count;
            m2 += delta * (value - mean);
        }

        while (count < numObservations) {
            int value = 0;
            count++;
            double delta = value - mean;
            mean += delta / count;
            m2 += delta * (value - mean);
        }

        if (count < 2) {
            stddev = 0.0;
        } else {
            stddev = Math.sqrt(m2 / count);
        }

        double delta = (measurement - (sigma * stddev + mean));

        VirtualTreeNode vchild, vparent;
        if (delta >= 0 && measurement >= minMeasurement) {
            List<DataTreeNode> result = new ArrayList<>();
            vchild = new VirtualTreeNode("threshold",
                    DoubleMath.roundToLong(sigma * stddev + mean, RoundingMode.HALF_UP));
            vparent = new VirtualTreeNode("stddev",
                    DoubleMath.roundToLong(stddev, RoundingMode.HALF_UP), generateSingletonArray(vchild));
            vchild = vparent;
            vparent = new VirtualTreeNode("mean",
                    DoubleMath.roundToLong(mean, RoundingMode.HALF_UP), generateSingletonArray(vchild));
            vchild = vparent;
            vparent = new VirtualTreeNode("measurement",
                    measurement, generateSingletonArray(vchild));
            vchild = vparent;
            vparent = new VirtualTreeNode("delta",
                    DoubleMath.roundToLong(delta, RoundingMode.HALF_UP), generateSingletonArray(vchild));
            result.add(vparent);
            if (raw) {
                addRawObservations(result);
            }
            return result;
        } else {
            return makeDefaultNodes(raw);
        }
    }

    @Override
    public byte[] bytesEncode(long version) {
        if (reservoir == null) {
            return EMPTY_BYTES;
        }
        byte[] retBytes = null;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            Varint.writeUnsignedVarLong(minEpoch, byteBuf);
            Varint.writeUnsignedVarInt(reservoir.length, byteBuf);
            for (int i = 0; i < reservoir.length; i++) {
                Varint.writeUnsignedVarInt(reservoir[i], byteBuf);
            }
            retBytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(retBytes);
        } finally {
            byteBuf.release();
        }
        return retBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        if (b.length == 0) {
            return;
        }
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);
        try {
            minEpoch = Varint.readUnsignedVarLong(byteBuf);
            int length = Varint.readUnsignedVarInt(byteBuf);
            reservoir = new int[length];
            for (int i = 0; i < reservoir.length; i++) {
                reservoir[i] = Varint.readUnsignedVarInt(byteBuf);
            }
        } finally {
            byteBuf.release();
        }
    }
}
