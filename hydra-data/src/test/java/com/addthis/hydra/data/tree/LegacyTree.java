package com.addthis.hydra.data.tree;

import java.io.File;
import java.io.IOException;

import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.IPageDB;
import com.addthis.hydra.store.db.LegacyPageDB;

public class LegacyTree extends Tree {

    /**
     * has deferred init() to allow for threaded allocation in Oracle
     *
     * @param root
     * @param readonly
     * @param init
     */
    public LegacyTree(File root, boolean readonly, boolean init) throws Exception {
        super(root, readonly, init);
    }

    protected IPageDB<DBKey, TreeNode> generatePageDB(File root, boolean readonly) throws IOException {
        return new LegacyPageDB<>(root, TreeNode.class, TreeCommonParameters.maxPageSize,
                TreeCommonParameters.maxCacheSize, readonly);
    }

}
