package com.addthis.hydra.common.hash;

import com.addthis.codec.annotations.Pluggable;

@Pluggable("hash function")
public interface StringAndByteHashFunction {

    public int hash(String input);

    public int hash(byte[] input);
}
