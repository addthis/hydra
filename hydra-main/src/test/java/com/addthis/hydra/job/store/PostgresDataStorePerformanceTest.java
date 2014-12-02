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
import java.util.Properties;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test for performance testing the Jdbc data stores. 
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 30)
@BenchmarkOptions(callgc = false, benchmarkRounds = 30, warmupRounds = 5)
public class PostgresDataStorePerformanceTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataStorePerformanceTest.class);
    
    private static PostgresDataStore jdbcDataStore;
    
    private static int putCount = 0;
    
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        //set up the connection
        final Properties properties = new Properties();
        properties.put("user", "performance");
        jdbcDataStore = new PostgresDataStore("jdbc:postgresql://localhost/", "junitPerformance", "tableName", properties);
    }

    @AfterClass
    public static void tearDownClass() {
        jdbcDataStore.close();
    }

    @Test
    public void testPut() throws Exception {
        final String path = "generatedPath/" + putCount++;
        final String value = RandomStringUtils.random(14144); //~40k worth of UTF8
        jdbcDataStore.put(path, value);
        LOG.info("Stored {} to {}", path, value);
    }
    
    @Test
    public void testGetAllChildren() throws Exception {
        final String path = "generatedPath/";
        final Map<String,String> childrenMap = jdbcDataStore.getAllChildren(path);
        LOG.info("Fetched {} child map records", childrenMap.size());
    }
    
    @Test
    public void testChildrenNames() throws Exception {
        final String path = "generatedPath/";
        final List<String> childrenNames = jdbcDataStore.getChildrenNames(path);
        LOG.info("Fetched {} child name records", childrenNames.size());
    }
    
}