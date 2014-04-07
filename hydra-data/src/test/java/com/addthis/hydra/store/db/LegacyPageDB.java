package com.addthis.hydra.store.db;

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.ByteStoreBDB;
import com.addthis.hydra.store.kv.LegacyExternalPagedStore;
import com.addthis.hydra.store.skiplist.PageFactory;

public class LegacyPageDB<V extends Codec.BytesCodable> extends PageDB<V> {

    public LegacyPageDB(File dir, Class<? extends V> clazz, int maxPageSize, int maxPages, boolean readonly)
            throws IOException {
        this(dir, clazz, defaultDbName, maxPageSize, maxPages, defaultKeyValueStoreType, readonly, null);
    }

    public LegacyPageDB(File dir, Class clazz, String dbname, int maxPageSize, int maxPages,
            int keyValueStoreType, boolean readonly, PageFactory factory) throws IOException {
        super(readonly, new LegacyExternalPagedStore(
                        new DBKeyCoder(clazz),
                        new ByteStoreBDB(dir, dbname, readonly), maxPageSize, maxPages),
                new DBKeyCoder(clazz));
        if (!readonly) {
            Files.write(new File(dir, "db.type"), Bytes.toBytes(getClass().getName()), false);
        }
    }
}
