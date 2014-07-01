package com.addthis.hydra.common.hash;

import com.google.common.hash.Hashing;

final class Murmur3HashFunction implements StringAndByteHashFunction {

    public int hash(String input) {
        return Hashing.murmur3_32().hashUnencodedChars(input).asInt();
    }

    public int hash(byte[] input) {
        return Hashing.murmur3_32().hashBytes(input).asInt();
    }
}
