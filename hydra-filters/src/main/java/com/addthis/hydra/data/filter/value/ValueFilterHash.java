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
package com.addthis.hydra.data.filter.value;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.addthis.basis.util.LessBytes;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.common.hash.PluggableHashFunction;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.commons.codec.binary.Hex;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns the hash of a value</span>.
 * <p/>
 * <p>The {@link #type type} field determines what type of hash is calculated.
 * <p>
 * <ul>
 * <li>"0" uses the java {@link Object#hashCode() hashCode()} method.</li>
 * <li>"1" uses the PluggableHashFunction method.</li>
 * <li>"2" uses the CUID hash method.</li>
 * <li>"3" uses SHA hashing.</li>
 * <li>"4" uses SHA-1 hashing.</li>
 * </ul>
 * </p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *      {from:"UID", hash.type:2}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterHash extends AbstractValueFilter {

    /**
     * The type of hashing method to use. Default is 1.
     */
    @FieldConfig(codable = true)
    private int type = 1;

    /**
     * If true, then return the absolute value of the calculated hash. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean abs;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return value;
        }
        long hash = 0;
        String sv = ValueUtil.asNativeString(value);
        switch (type) {
            case 0:
                hash = sv.hashCode();
                break;
            case 1:
                hash = PluggableHashFunction.hash(sv);
                break;
            case 2:
                hash = cuidHash(sv);
                break;
            case 3:
                try {
                    MessageDigest md = MessageDigest.getInstance("SHA");
                    md.update(LessBytes.toBytes(sv));
                    byte[] b = md.digest();
                    for (int i = 0; i < b.length && i < 8; i++) {
                        hash = (hash << 8) | (b[i] & 0xff);
                    }
                } catch (NoSuchAlgorithmException e) {
                    // ignore
                }
                break;
            case 4:
                try {
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.reset();
                    md.update(LessBytes.toBytes(sv));
                    return ValueFactory.create(new String(Hex.encodeHex(md.digest())));
                } catch (NoSuchAlgorithmException e) {
                    // ignore
                }
            case 5:
                HashFunction hashAlgorithm = Hashing.murmur3_128();
                HashCode hashCode = hashAlgorithm.newHasher().putString(sv, Charsets.UTF_8).hash();
                hash = hashCode.asLong();
                break;
            default:
                throw new RuntimeException("Unknown hash type: " + type);
        }
        if (abs) {
            hash = Math.abs(hash);
        }
        return ValueFactory.create(hash);
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setAbs(boolean abs) {
        this.abs = abs;
    }

    // PluggableHashFunction based cuidHash; originally from CUID
    // Required for some job compatibility
    // TODO: Why did we have this crazy custom hash function?
    public static long cuidHash(String cuid) {
        long hi = PluggableHashFunction.hash(cuid) & Integer.MAX_VALUE;//& 0xffffffffL;
        long lo = PluggableHashFunction.hash(new StringBuilder(cuid).reverse().toString()) & 0xffffffffL;
        return ((hi << 32) | lo);
    }

}
