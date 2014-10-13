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

import com.addthis.basis.util.Bytes;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.BytesCodable;


public final class Raw implements Comparable<Raw>, BytesCodable {

    private static final boolean padprintable = System.getProperty("abyss.raw.padprintable", "0").equals("1");
    private static boolean longcompare = System.getProperty("abyss.raw.longcompare", "0").equals("1");
    private static final boolean showinfo = System.getProperty("abyss.raw.showinfo", "0").equals("1");

    static {
        if (showinfo) {
            System.out.println("Raw padprintable=" + padprintable + " longcompare=" + longcompare);
        }
    }

    public static void useLongCompare(boolean b) {
        longcompare = b;
    }

    public static final Raw get(String s) {
        return new Raw(s);
    }

    public static final Raw get(char[] c) {
        return new Raw(Bytes.toBytes(c));
    }

    public static final Raw get(byte[] b) {
        return new Raw(b);
    }

    private Raw(String s) {
        this.raw = Bytes.toBytes(s);
    }

    private Raw(byte[] b) {
        this.raw = b;
    }

    public Raw() {
    }

    @FieldConfig(codable = true)
    private byte[] raw;
    private int    hashcode;
    private long[] compare;

    @Override
    public int hashCode() {
        if (hashcode == 0) {
            int h = 0;
            byte[] val = raw;
            for (int i = 0, l = val.length; i < l; i++) {
                h = 31 * h + val[i];
            }
            hashcode = h;
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o.getClass() == Raw.class) {
            Raw ro = (Raw) o;
            return (o == this) || revsame(raw, ro.raw);
        } else {
            return false;
        }
    }

    // compare in reverse
    private static boolean revsame(byte[] a, byte[] b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.length != b.length) {
            return false;
        }
        for (int l = a.length - 1, i = l; i >= 0; i--) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    // compare forward
    private static boolean fwdsame(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int l = a.length, i = 0; i < l; i++) {
            if ((int) a[i] != (int) b[i]) {
                return false;
            }
        }
        return true;
    }

    public String toPrintable() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; raw != null && i < raw.length; i++) {
            if (raw[i] < 32 || raw[i] > 126) {
                sb.append("(" + ((int) (raw[i] & 0xff)) + ")");
            } else {
                sb.append(((char) (raw[i] & 0xff)));
            }
            if (padprintable) {
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    public String toString() {
        return Bytes.toString(raw);
    }

    public char[] toChars() {
        return Bytes.toChars(raw);
    }

    public byte[] toBytes() {
        return raw;
    }

    public Raw cat(byte[] b) {
        return get(Bytes.cat(raw, b));
    }

    public Raw cat(String b) {
        return get(Bytes.cat(raw, Bytes.toBytes(b)));
    }

    public Raw cat(Raw ab) {
        return cat(ab.raw);
    }

    @Override public int compareTo(Raw o) {
        if (longcompare) {
            return compare(getLongs(), o.getLongs());
        }
        return Bytes.compare(raw, o.raw);
    }

    private long[] getLongs() {
        if (compare == null) {
            compare = bytesToLong(raw);
        }
        return compare;
    }

    private int compare(long[] a, long[] b) {
        for (int al = a.length, bl = b.length, i = 0; i < al; i++) {
            if (bl <= i) {
                return 1;
            }
            long val = a[i] - b[i];
            if (val == 0) {
                continue;
            }
            if (val < 0) {
                return -1;
            }
            return 1;
        }
        return a.length == b.length ? 0 : -1;
    }

    private long[] bytesToLong(byte[] data) {
        if (data.length % 8 != 0) {
            data = Bytes.cat(data, new byte[8 - (data.length % 8)]);
        }
        long[] l = new long[data.length / 8];
        for (int i = 0; i < l.length; i++) {
            int off = i * 8;
            l[i] = (long) (
                    ((data[off] & 0xffL) << 56) | ((data[off + 1] & 0xffL) << 48) |
                    ((data[off + 2] & 0xffL) << 40) | ((data[off + 3] & 0xffL) << 32) |
                    ((data[off + 4] & 0xffL) << 24) | ((data[off + 5] & 0xffL) << 16) |
                    ((data[off + 6] & 0xffL) << 8) | ((data[off + 7] & 0xffL)));
        }
        return l;
    }

    @Override
    public byte[] bytesEncode(long version) {
        return toBytes();
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        this.raw = b;
    }
}
