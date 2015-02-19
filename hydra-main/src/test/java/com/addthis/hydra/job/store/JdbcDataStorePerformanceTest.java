/*
 * Copyright 2014 AddThis.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.job.store;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test for performance testing the Jdbc data stores. 
 */
public abstract class JdbcDataStorePerformanceTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataStorePerformanceTest.class);
    
    public abstract JdbcDataStore getJdbcDataStore();
    
    private static int putCount = 1;
    
    private static final Random RANDOM = new Random();
    
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @Test
    public void testPut() throws Exception {
        final String path = "generatedPath/" + putCount++;
        final String value = RandomStringUtils.random(14144,"abcdefghijlkmnopqrstuvwxyz1234567890"); //~40k worth of UTF8
        getJdbcDataStore().put(path, value);
        LOG.info("Stored {}", path, value);
    }
    
    @Test
    public void testRePut() throws Exception {
        final String path = "generatedPath/" + RANDOM.nextInt(putCount);
        final String value = RandomStringUtils.random(14144,"abcdefghijlkmnopqrstuvwxyz1234567890"); //~40k worth of UTF8
        getJdbcDataStore().put(path, value);
        LOG.info("Stored {}", path, value);
    }
    
    @Test
    public void testGet() throws Exception {
        final String path = "generatedPath/" + RANDOM.nextInt(putCount);
        final String value = getJdbcDataStore().get(path);
        LOG.info("Fetched value for path", path);
    }
    
    @Test
    public void testGetAllChildren() throws Exception {
        final String path = "generatedPath/" + RANDOM.nextInt(putCount);
        final Map<String,String> childrenMap = getJdbcDataStore().getAllChildren(path);
        LOG.info("Fetched {} child map records", childrenMap.size());
    }
    
    @Test
    public void testChildrenNames() throws Exception {
        final String path = "generatedPath/" + RANDOM.nextInt(putCount);
        final List<String> childrenNames = getJdbcDataStore().getChildrenNames(path);
        LOG.info("Fetched {} child name records", childrenNames.size());
    }
    
//    @Test
//    public void testGetRootChildrenNames() throws Exception {
//        final List<String> childrenNames = getJdbcDataStore().getChildrenNames(JdbcDataStore.blankChildValue);
//        LOG.info("Fetched {} child name records", childrenNames.size());
//    }
    
    @Test
    public void testDelete() throws Exception {
        final String path = "generatedPath/" + putCount--;
        getJdbcDataStore().delete(path);
        LOG.info("Deleted {}", path);
    }
}
