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

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author brad
 */
public class MysqlDataStoreTest {
    
    private static final String DB_URL = "mock:driver/";
    
    //mocks
    private static String originalDriver;
    private static Driver driver;
    /** Container for our actual mock that the {@link DriverManager} can instantiate. */
    private final MockDriver mockDriver = new MockDriver();
    private Connection connection;
    

    //class under test
    private MysqlDataStore mysqlDataStore;
   
    
    public MysqlDataStoreTest() {
    }
    
    
    @BeforeClass
    public static void setUpClass() {
        //capture the original setting to revert it after the test is complete
        originalDriver = System.getProperty("sql.datastore.driverclass");
        System.setProperty("sql.datastore.driverclass", MockDriver.class.getName());
    }
    
    @AfterClass
    public static void tearDownClass() {
        //revert our overridden property
        if (originalDriver == null) {
            System.clearProperty("sql.datastore.driverclass");
        } else {
            System.setProperty("sql.datastore.driverclass", originalDriver);
        }
    }
    
    @Before
    public void setUp() throws Exception {
        //register the driver
        DriverManager.registerDriver(mockDriver);
        
        //set up the mocked driver
        driver = Mockito.mock(Driver.class);
        connection = Mockito.mock(Connection.class);
        Mockito.when(driver.acceptsURL(Mockito.startsWith(DB_URL))).thenReturn(Boolean.TRUE); //tell the driver manager we can handle this url
        Mockito.when(driver.connect(Mockito.startsWith(DB_URL), Mockito.any(Properties.class))).thenReturn(connection); //go ahead and get a connection
        final PreparedStatement createDatabasePreparedStatement = Mockito.mock(PreparedStatement.class); //temporary mock for creating the database
        Mockito.when(connection.prepareStatement(Mockito.startsWith("CREATE DATABASE IF NOT EXISTS"))).thenReturn(createDatabasePreparedStatement);
        Mockito.when(createDatabasePreparedStatement.execute()).thenReturn(Boolean.TRUE);
        final PreparedStatement createTablePreparedStatement = Mockito.mock(PreparedStatement.class); //temporary mock for creating the table
        Mockito.when(connection.prepareStatement(Mockito.startsWith("CREATE TABLE IF NOT EXISTS"))).thenReturn(createTablePreparedStatement);
        
        //create our class under test
        final Properties properties = new Properties();
        mysqlDataStore = new MysqlDataStore(DB_URL, "dbName", "tableName", properties);
        
        //verify construction
        Mockito.verify(driver, Mockito.atLeastOnce()).connect(Mockito.startsWith(DB_URL), Mockito.any(Properties.class));
        Mockito.verify(createDatabasePreparedStatement).execute(); //make sure we called execute.  This also verifies that the prepared statement was correctly constructed.
        Mockito.verify(createTablePreparedStatement).execute(); //make sure we called execute.  This also verifies that the prepared statement was correctly constructed.
    }
    
    @After
    public void tearDown() throws SQLException {
        //run some verifications
        Mockito.verify(driver).acceptsURL(Mockito.startsWith(DB_URL));
        
        //shut down the connection pooling
        if (mysqlDataStore != null) {
            mysqlDataStore.close();
            Mockito.verify(connection).close(); //this tests the close method as well
        }
        
        //clear the mocks
        Mockito.reset(driver, connection);
        
        //deregister the driver we started out with
        DriverManager.deregisterDriver(mockDriver);
    }

    /**
     * Test of getDescription method, of class MysqlDataStore.
     */
    @Test
    public void testGetDescription() {
        assertEquals("mysql", mysqlDataStore.getDescription());
    }

    @Test
    public void testGet_String() throws SQLException {
        //set up data
        final String value = "value";
        
        //set up mocks
        final PreparedStatement selectPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq("SELECT val FROM tableName WHERE path=? AND child=?"))).thenReturn(selectPreparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(selectPreparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(Boolean.TRUE, Boolean.FALSE);
        final Blob blob = Mockito.mock(Blob.class);
        Mockito.when(resultSet.getBlob(1)).thenReturn(blob);
        Mockito.when(blob.getBytes(Mockito.anyLong(), Mockito.anyInt())).thenReturn(LZFEncoder.encode(value.getBytes()));
        
        //run method under test
        final String result = mysqlDataStore.get("key");
        
        //assertions
        assertNotNull(result);
        assertEquals(value, result);
        
        //verifications
        Mockito.verify(selectPreparedStatement, Mockito.atLeastOnce()).executeQuery();
    }

    @Test
    public void testGet_StringArr() throws SQLException {
        //set up data
        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");
        
        //set up mocks
        final PreparedStatement selectPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("SELECT path,val FROM tableName WHERE child=? AND path IN"))).thenReturn(selectPreparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(selectPreparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        final List<String> keyList = new ArrayList<>(expected.keySet());
        final Blob blob = Mockito.mock(Blob.class);
        Mockito.when(resultSet.getString("path")).thenReturn(keyList.get(0), keyList.get(1), keyList.get(2));
        Mockito.when(resultSet.getBlob("val")).thenReturn(blob);
        Mockito.when(blob.getBytes(Mockito.anyLong(), Mockito.anyInt())).thenReturn(
                LZFEncoder.encode(expected.get(keyList.get(0)).getBytes()),
                LZFEncoder.encode(expected.get(keyList.get(1)).getBytes()),
                LZFEncoder.encode(expected.get(keyList.get(2)).getBytes()));
        
        //run method under test
        final Map<String, String> result = mysqlDataStore.get(keyList.toArray(new String[] {}));
        
        //assertions
        assertNotNull(result);
        expected.keySet().stream().forEach((key) -> {
            //iterate over the expected keys, which also makes sure they are all there
            assertEquals(expected.get(key), result.get(key));
        });
        
        //verifications
        Mockito.verify(selectPreparedStatement, Mockito.atLeastOnce()).executeQuery();
    }

    @Test
    public void testPut() throws Exception {
        //set up mocks
        final PreparedStatement insertPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("REPLACE INTO "))).thenReturn(insertPreparedStatement);
        Mockito.doAnswer((Answer) (InvocationOnMock invocation) -> {
            final Blob blob = (Blob)invocation.getArguments()[1];
            assertEquals(new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))), "value");
            return null;
        }).when(insertPreparedStatement).setBlob(Mockito.eq(2), Mockito.any(Blob.class));
        
        //run method under test
        mysqlDataStore.put("key", "value");
        
        //verifications
        Mockito.verify(insertPreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(insertPreparedStatement).setBlob(Mockito.eq(2), Mockito.any(Blob.class));
        Mockito.verify(insertPreparedStatement).setString(Mockito.eq(3), Mockito.eq("_root"));
        Mockito.verify(insertPreparedStatement, Mockito.atLeastOnce()).execute();
    }

    @Test
    public void testPutAsChild() throws Exception {
        //set up mocks
        final PreparedStatement insertPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("REPLACE INTO "))).thenReturn(insertPreparedStatement);
        Mockito.doAnswer((Answer) (InvocationOnMock invocation) -> {
            final Blob blob = (Blob)invocation.getArguments()[1];
            assertEquals(new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))), "value");
            return null;
        }).when(insertPreparedStatement).setBlob(Mockito.eq(2), Mockito.any(Blob.class));
        
        //run method under test
        mysqlDataStore.putAsChild("key", "childId", "value");
        
        //verifications
        Mockito.verify(insertPreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(insertPreparedStatement).setBlob(Mockito.eq(2), Mockito.any(Blob.class));
        Mockito.verify(insertPreparedStatement).setString(Mockito.eq(3), Mockito.eq("childId"));
        Mockito.verify(insertPreparedStatement, Mockito.atLeastOnce()).execute();
    }

    /**
     * Test of getChild method, of class MysqlDataStore.
     */
    @Test
    public void testGetChild() throws Exception {
        //set up data
        final String value = "value";
        
        //set up mocks
        final PreparedStatement selectPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq("SELECT val FROM tableName WHERE path=? AND child=?"))).thenReturn(selectPreparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(selectPreparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(Boolean.TRUE, Boolean.FALSE);
        final Blob blob = Mockito.mock(Blob.class);
        Mockito.when(resultSet.getBlob(1)).thenReturn(blob);
        Mockito.when(blob.getBytes(Mockito.anyLong(), Mockito.anyInt())).thenReturn(LZFEncoder.encode(value.getBytes()));
        
        //run method under test
        final String result = mysqlDataStore.getChild("key", "childId");
        
        //assertions
        assertNotNull(result);
        assertEquals(value, result);
        
        //verifications
        Mockito.verify(selectPreparedStatement, Mockito.atLeastOnce()).executeQuery();
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(2), Mockito.eq("childId"));
    }

    @Test
    public void testDeleteChild() throws SQLException {
        //set up mocks
        final PreparedStatement deletePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("DELETE FROM "))).thenReturn(deletePreparedStatement);
        
        //run method under test
        mysqlDataStore.deleteChild("key", "childId");
        
        //verifications
        Mockito.verify(deletePreparedStatement, Mockito.atLeastOnce()).execute();
        Mockito.verify(deletePreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(deletePreparedStatement).setString(Mockito.eq(2), Mockito.eq("childId"));
    }

    @Test
    public void testDelete() throws SQLException {
        //set up mocks
        final PreparedStatement deletePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("DELETE FROM "))).thenReturn(deletePreparedStatement);
        
        //run method under test
        mysqlDataStore.delete("key");
        
        //verifications
        Mockito.verify(deletePreparedStatement, Mockito.atLeastOnce()).execute();
        Mockito.verify(deletePreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
    }

    /**
     * Test of getChildrenNames method, of class MysqlDataStore.
     */
    @Test
    public void testGetChildrenNames() throws SQLException {
        //set up mocks
        final PreparedStatement selectPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.startsWith("SELECT DISTINCT "))).thenReturn(selectPreparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(selectPreparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE); //two results
        Mockito.when(resultSet.getString(1)).thenReturn("value1", "value2");
        
        //run method under test
        final List<String> result = mysqlDataStore.getChildrenNames("key");
        
        //assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("value1", result.get(0));
        assertEquals("value2", result.get(1));
        
        //verifications
        Mockito.verify(selectPreparedStatement, Mockito.atLeastOnce()).executeQuery();
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(2), Mockito.eq("_root"));
    }

    @Test
    public void testGetAllChildren() throws SQLException {
        //set up data
        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");
        
        //set up mocks
        final PreparedStatement selectPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq("SELECT child,val FROM tableName WHERE path=? AND child!=?"))).thenReturn(selectPreparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(selectPreparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        final List<String> keyList = new ArrayList<>(expected.keySet());
        final Blob blob = Mockito.mock(Blob.class);
        Mockito.when(resultSet.getString(1)).thenReturn(keyList.get(0), keyList.get(1), keyList.get(2));
        Mockito.when(resultSet.getBlob(2)).thenReturn(blob);
        Mockito.when(blob.getBytes(Mockito.anyLong(), Mockito.anyInt())).thenReturn(
                LZFEncoder.encode(expected.get(keyList.get(0)).getBytes()),
                LZFEncoder.encode(expected.get(keyList.get(1)).getBytes()),
                LZFEncoder.encode(expected.get(keyList.get(2)).getBytes()));
        
        //run method under test
        final Map<String, String> result = mysqlDataStore.getAllChildren("key");
        
        //assertions
        assertNotNull(result);
        expected.keySet().stream().forEach((key) -> {
            //iterate over the expected keys, which also makes sure they are all there
            assertEquals(expected.get(key), result.get(key));
        });
        
        //verifications
        Mockito.verify(selectPreparedStatement, Mockito.atLeastOnce()).executeQuery();
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(1), Mockito.eq("key"));
        Mockito.verify(selectPreparedStatement).setString(Mockito.eq(2), Mockito.eq("_root"));
    }
    
    /**
     * We need the qualified string representation of a concrete class that is a jdbc driver.  This will just 
     * defer to the actual mocked version managed by Mockito that the unit test has access to.
     */
    public class MockDriver implements Driver {
        
        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return driver.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return driver.acceptsURL(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return driver.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return driver.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return driver.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return driver.jdbcCompliant();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return driver.getParentLogger();
        }
        
    }
    
}
