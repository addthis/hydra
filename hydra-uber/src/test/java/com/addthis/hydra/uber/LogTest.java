package com.addthis.hydra.uber;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.Test;

public class LogTest {

    @Test
    public void logSimple() {
        Logger log = LogManager.getLogger();
        log.info("should appear only in stdout");
        log.warn("should appear in both stdout and stderr");
    }
}