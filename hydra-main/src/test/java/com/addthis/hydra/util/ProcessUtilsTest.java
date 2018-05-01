package com.addthis.hydra.util;

import com.addthis.hydra.minion.ProcessUtils;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;

public class ProcessUtilsTest {

    @Test
    public void testActiveProcessExistsWithPid() {
        // this should get pid of current java process, but not guaranteed to be portable
        // see: https://stackoverflow.com/a/35885
        // reimplement this if it causes problems
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        Integer pid = Integer.valueOf(processName.split("@")[0]);
        assert(ProcessUtils.activeProcessExistsWithPid(pid, new File(".")));

        assert(!ProcessUtils.activeProcessExistsWithPid(0, new File(".")));
    }
}
