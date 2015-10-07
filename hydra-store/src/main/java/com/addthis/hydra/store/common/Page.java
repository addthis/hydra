package com.addthis.hydra.store.common;

import java.util.ArrayList;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.skiplist.LockMode;

import io.netty.buffer.ByteBufOutputStream;

public interface Page<K, V extends BytesCodable> {
    void initialize();

    byte[] encode(ByteBufOutputStream out);

    byte[] encode(ByteBufOutputStream out, boolean record);

    void decode(byte[] page);

    int getMemoryEstimate();

    void updateMemoryEstimate();

    boolean interval(Comparable<? super K> ckey);

    void fetchValue(int position);

    boolean splitCondition();

    boolean inTransientState();

    PageEncodeType getEncodeType();

    ExternalMode getState();

    void setState(ExternalMode externalMode);

    ArrayList<K> keys();

    ArrayList<V> values();

    ArrayList<byte[]> rawValues();

    K getFirstKey();

    int size();

    void setSize(int size);

    K getNextFirstKey();

    void setNextFirstKey(K nextFirstKey);

    int getAvgEntrySize();

    void setAvgEntrySize(int avgEntrySize);

    int getEstimates();

    void setEstimates(int estimates);

    int getEstimateTotal();

    void setEstimateTotal(int estimateTotal);

    void setTimeStamp(long timeStamp);

    void setKeys(ArrayList<K> keys);

    void setValues(ArrayList<V> values);

    void setRawValues(ArrayList<byte[]> rawValues);

    long getTimeStamp();

    void updateAverage(K key, V val, int count);

    boolean writeTryLock();

    void writeLock();

    boolean isWriteLockedByCurrentThread();

    boolean isReadLockedByCurrentThread();

    void readUnlock();

    void modeUnlock(LockMode currentMode);

    void downgradeLock();

    void modeLock(LockMode currentMode);

    void writeUnlock();

    void readLock();

    long getWriteStamp();

    void incrementWriteStamp();

}
