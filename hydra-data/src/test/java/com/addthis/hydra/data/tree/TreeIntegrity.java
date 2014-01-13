package com.addthis.hydra.data.tree;

import java.io.File;

import com.addthis.hydra.store.skiplist.SkipListCache;

public class TreeIntegrity {


    public static void main(String[] args) {

        File root = new File(args[0]);
        try {
            ConcurrentTree tree = new ConcurrentTree(root, true);
            tree.testIntegrity();
        } catch(Exception ex) {
            System.err.println(ex);
        }
    }


}
