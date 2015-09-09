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
package com.addthis.hydra.data.filter.closeablebundle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueObject;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import com.google.common.io.ByteStreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This closeable bundle filter <span class="hydra-summary">applies a limit using the count-min sketch data structure</span>.
 * If the input is a scalar value then this filter returns true if it accepts the input. If
 * it rejects the input then that field is removed from the bundle and the filter returns false.
 * If the input is an array then this filter removes values that do not need the limit criteria
 * and always return true.
 *
 * @user-reference
 */
public class CloseableBundleCMSLimit implements CloseableBundleFilter {

    public enum Bound {
        LOWER, UPPER;
    }

    private static final String KEY_SEPARATOR = "&";

    @Nonnull
    public final AutoField[] keyFields;

    @Nonnull
    public final AutoField valueField;

    @Nullable
    public final AutoField countField;

    public final String dataDir;

    public final int cacheSize;

    public final boolean rejectNull;

    public final int limit;

    /**
     * The value to return if the filter removes the item from the bundle.
     * Only used if the input is a scalar. If the input is an array then
     * always return true. Default value of parameter is false.
     */
    public final boolean failReturn;

    /**
     * Optionally specify the depth of the sketch.
     * If 'confidence' is specified then ignore this value.
     */
    public final int depth;

    /**
     * Confidence that the error tolerance is satisfied.
     * If 'confidence' is specified then ignore 'depth' parameter.
     * Expressed as a fraction.
     */
    public final double confidence;

    /**
     * Width of the sketch in bits.
     * Either 'width' or 'percentage' are required.
     */
    public final int width;

    /**
     * Maximum error tolerated as percentage of cardinality.
     * Either 'width' or 'percentage' are required.
     */
    public final double percentage;

    @Nonnull
    public final Bound bound;

    private final CMSLimitHashMap sketches;

    private final int calcWidth;

    private final int calcDepth;

    @JsonCreator
    public CloseableBundleCMSLimit(@JsonProperty(value = "keyFields", required = true) AutoField[] keyFields,
                                   @JsonProperty(value = "valueField", required = true) AutoField valueField,
                                   @JsonProperty("countField") AutoField countField,
                                   @JsonProperty(value = "dataDir", required = true) String dataDir,
                                   @JsonProperty(value = "cacheSize", required = true) int cacheSize,
                                   @JsonProperty("rejectNull") boolean rejectNull,
                                   @JsonProperty("failReturn") boolean failReturn,
                                   @JsonProperty("width") int width,
                                   @JsonProperty("depth") int depth,
                                   @JsonProperty(value = "limit", required = true) int limit,
                                   @JsonProperty("confidence") double confidence,
                                   @JsonProperty("percentage") double percentage,
                                   @JsonProperty(value = "bound", required = true) Bound bound) {
        if ((width == 0) && (percentage == 0.0)) {
            throw new IllegalArgumentException("Either 'width' or " +
                                               "'percentage' must be specified.");
        } else if ((width > 0) && (percentage > 0.0)) {
            throw new IllegalArgumentException("Either 'width' or " +
                                               "'percentage' must be specified.");
        } else if (confidence < 0.0 || confidence >= 1.0) {
            throw new IllegalArgumentException("'confidence' must be between 0 and 1");
        }
        this.keyFields = keyFields;
        this.valueField = valueField;
        this.countField = countField;
        this.dataDir = dataDir;
        this.cacheSize = cacheSize;
        this.rejectNull = rejectNull;
        this.failReturn = failReturn;
        this.width = width;
        this.depth = depth;
        this.limit = limit;
        this.confidence = confidence;
        this.percentage = percentage;
        this.bound = bound;
        this.sketches = new CMSLimitHashMap();
        int cWidth = width;
        int cDepth = depth;
        if (cWidth == 0) {
            cWidth =  (int) Math.ceil(Math.E / percentage);
        }
        if (confidence > 0.0) {
            cDepth = (int) Math.ceil(-Math.log(1.0 - confidence));
        }
        calcWidth = cWidth;
        calcDepth = cDepth;
    }

    @Override public synchronized void close() {
        try {
            for (Map.Entry<String, CountMinSketch> entry : sketches.entrySet()) {
                writeSketch(entry.getKey(), entry.getValue());
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override public boolean filter(Bundle row) {
        StringBuilder sb = new StringBuilder();
        for (AutoField keyField : keyFields) {
            Optional<String> optional = keyField.getString(row);
            if (optional.isPresent()) {
                if (sb.length() > 0) {
                    sb.append(KEY_SEPARATOR);
                }
                sb.append(optional.get());
            } else if (rejectNull) {
                return failReturn;
            }
        }
        return updateSketch(row, sb.toString(), valueField.getValue(row));
    }

    private synchronized boolean updateSketch(Bundle row, String key, ValueObject valueObject) {
        CountMinSketch sketch = sketches.get(key);
        if (valueObject == null) {
            return failReturn;
        }
        if (valueObject.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray array = valueObject.asArray();
            Iterator<ValueObject> iterator = array.iterator();
            while (iterator.hasNext()) {
                ValueObject next = iterator.next();
                updateString(next.asString().asNative(), sketch, iterator, row);
            }
            return true;
        } else {
            return updateString(valueObject.asString().asNative(), sketch, null, row);
        }
    }

    private boolean updateString(String input, CountMinSketch sketch, Iterator<ValueObject> iterator, Bundle bundle) {
        long current = sketch.estimateCount(input);
        switch (bound) {
            case UPPER:
                if (current > limit) {
                    removeElement(iterator, bundle);
                    return failReturn;
                } else {
                    updateCount(input, sketch, bundle);
                }
                break;
            case LOWER:
                if (current < limit) {
                    removeElement(iterator, bundle);
                    updateCount(input, sketch, bundle);
                    return failReturn;
                }
                break;
        }
        return true;
    }

    private void removeElement(Iterator<ValueObject> iterator, Bundle bundle) {
        if (iterator != null) {
            iterator.remove();
        } else {
            valueField.removeValue(bundle);
        }
    }

    private void updateCount(String input, CountMinSketch sketch, Bundle bundle) {
        long myCount = 1;
        if (countField != null) {
            myCount = countField.getLong(bundle).orElse(0);
        }
        if (myCount > 0) {
            sketch.add(input, myCount);
        }
    }

    private void writeSketch(String key, CountMinSketch sketch) throws IOException {
        byte[] data = CountMinSketch.serialize(sketch);
        ByteArrayOutputStream byteStream =
                new ByteArrayOutputStream(data.length);
        GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
        try {
            zipStream.write(data);
        } finally {
            zipStream.close();
            byteStream.close();
        }
        Path parent = Paths.get(dataDir);
        Path path = Paths.get(dataDir, key + ".gz");
        Files.createDirectories(parent);
        Files.write(path, byteStream.toByteArray());
    }

    private class CMSLimitHashMap extends LinkedHashMap<String, CountMinSketch> {

        CMSLimitHashMap() {
            super(cacheSize, 0.75f, true);
        }

        @Override
        public CountMinSketch get(Object key) {
            try {
                CountMinSketch sketch = super.get(key);
                if (sketch == null) {
                    ByteArrayInputStream byteStream = null;
                    GZIPInputStream zipStream = null;
                    try {
                        Path path = Paths.get(dataDir, key + ".gz");
                        if (Files.exists(path)) {
                            byte[] data = Files.readAllBytes(path);
                            byteStream = new ByteArrayInputStream(data);
                            zipStream = new GZIPInputStream(byteStream);
                            sketch = CountMinSketch.deserialize(ByteStreams.toByteArray(zipStream));
                        } else {
                            sketch = new CountMinSketch(calcDepth, calcWidth, 0);
                        }
                    } finally {
                        if (zipStream != null) {
                            zipStream.close();
                        }
                        if (byteStream != null) {
                            byteStream.close();
                        }
                    }
                    put(key.toString(), sketch);
                }
                return sketch;
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        protected boolean removeEldestEntry(Map.Entry<String, CountMinSketch> eldest) {
            try {
                if (size() > cacheSize) {
                    String key = eldest.getKey();
                    CountMinSketch value = eldest.getValue();
                    writeSketch(key, value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    public static class CloseableBundleCMSLimitBuilder {
        private AutoField[] keyFields;
        private AutoField valueField;
        private AutoField countField;
        private String dataDir;
        private int cacheSize;
        private boolean rejectNull;
        private int limit;
        private boolean failReturn;
        private int depth;
        private double confidence;
        private int width;
        private double percentage;
        private Bound bound;

        public CloseableBundleCMSLimitBuilder() {}

        public CloseableBundleCMSLimitBuilder(CloseableBundleCMSLimit source) {
            this.keyFields = source.keyFields;
            this.valueField = source.valueField;
            this.countField = source.countField;
            this.dataDir = source.dataDir;
            this.cacheSize = source.cacheSize;
            this.rejectNull = source.rejectNull;
            this.limit = source.limit;
            this.failReturn = source.failReturn;
            this.depth = source.depth;
            this.confidence = source.confidence;
            this.width = source.width;
            this.percentage = source.percentage;
            this.bound = source.bound;
        }

        public CloseableBundleCMSLimitBuilder setKeyFields(AutoField[] keyFields) {
            this.keyFields = keyFields;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setValueField(AutoField valueField) {
            this.valueField = valueField;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setCountField(AutoField countField) {
            this.countField = countField;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setDataDir(String dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setRejectNull(boolean rejectNull) {
            this.rejectNull = rejectNull;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setFailReturn(boolean failReturn) {
            this.failReturn = failReturn;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setDepth(int depth) {
            this.depth = depth;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setConfidence(double confidence) {
            this.confidence = confidence;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setWidth(int width) {
            this.width = width;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setPercentage(double percentage) {
            this.percentage = percentage;
            return this;
        }

        public CloseableBundleCMSLimitBuilder setBound(Bound bound) {
            this.bound = bound;
            return this;
        }

        public CloseableBundleCMSLimit build() {
            CloseableBundleCMSLimit closeableBundleCMSLimit =
                    new CloseableBundleCMSLimit(keyFields, valueField, countField, dataDir,
                                                cacheSize, rejectNull, failReturn, width,
                                                depth, limit, confidence, percentage, bound);
            return closeableBundleCMSLimit;
        }
    }


}
