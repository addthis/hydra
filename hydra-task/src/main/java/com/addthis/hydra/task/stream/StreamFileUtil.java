package com.addthis.hydra.task.stream;

public class StreamFileUtil {

    /**
     * Get the canonical form of the mesh file name based on the specified sort/path token offsets
     */
    public static String getCanonicalFileReferenceCacheKey(String name, int pathOff, String sortToken, int pathTokenOffset) {
        if (sortToken != null && pathTokenOffset > 0) {
            int pos = 0;
            int off = pathTokenOffset;
            while (off-- > 0 && (pos = name.indexOf(sortToken, pos)) >= 0) {
                pos++;
            }
            if (pos > 0) {
                pathOff += pos;
            }
        }
        return name.substring(pathOff);
    }
}
