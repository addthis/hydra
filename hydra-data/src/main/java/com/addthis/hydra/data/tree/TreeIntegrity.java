package com.addthis.hydra.data.tree;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeIntegrity {

    private static final Logger log = LoggerFactory.getLogger(TreeIntegrity.class);

    public static void main(String[] args) {
        File root = new File(args[0]);
        if (args.length > 1 && args[1].equals("repair")) {
            ConcurrentTree tree = null;
            try {
                tree = new ConcurrentTree(root);
                tree.repairIntegrity();
            } catch (Exception ex) {
                log.error(ex.toString());
            } finally {
                if (tree != null) tree.close();
            }
        } else {
            ReadTree tree = null;
            try {
                tree = new ReadTree(root);
                tree.testIntegrity();
            } catch(Exception ex) {
                log.error(ex.toString());
            } finally {
                if (tree != null) tree.close();
            }
        }
    }


}
