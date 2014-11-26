/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.job.store;

import javax.sql.rowset.serial.SerialBlob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.addthis.basis.util.Parameter;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 *
 */
public abstract class JdbcDataStore implements SpawnDataStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcDataStore.class);

    /* The simulated 'child' value used to store data about the parent. */
    protected static final String blankChildValue = "_root";

    /* Configuration parameters for the jdbc connection pool. */
    protected static final int minPoolSize = Parameter.intValue("sql.datastore.minpoolsize", 5);
    protected static final int maxPoolSize = Parameter.intValue("sql.datastore.maxpoolsize", 10);
    /* Number of times to retry inserts after a rollback exception */
    protected static final int insertRetries = Parameter.intValue("sql.datastore.insertRetries", 5);
    /* Parameters to retry and retire connections */
    protected static final int acquireRetries = Parameter.intValue("sql.datastore.acquireRetries", 10);
    protected static final int acquireDelay = Parameter.intValue("sql.datastore.acquireDelay", 5000); // ms
    protected static final int maxConnectionAge = Parameter.intValue("sql.datastore.maxConnectionAgeSeconds", 300); // = 5 minutes
    /* Performance metrics */
    /* Note that these assume there will be only one JDBC implementation running at a time.  Otherwise, 
     the metrics will be mixed between the implementations at runtime */
    private static final Timer queryTimer = Metrics.newTimer(JdbcDataStore.class, "jdbcQueryTime");
    private static final Timer insertTimer = Metrics.newTimer(JdbcDataStore.class, "jdbcInsertTime");
    private static final Counter errorCounter = Metrics.newCounter(JdbcDataStore.class, "jdbcErrors");
    /* The maximum allowable length for 'path' and 'child' values. */
    private static final int maxPathLength = Parameter.intValue("sql.datastore.max.path.length", 150);

    protected final String tableName;

    protected final ComboPooledDataSource cpds;

    /* Column names. Using default parameters, path and child are VARCHAR(150) and value is a BLOB. */
    private static final String pathKey = "path";
    private static final String valueKey = "val";
    private static final String childKey = "child";
    private static final String idKey = "id";

    /**
     * Create the data pool, initialize the connection pool, and create the
     * table if necessary.
     *
     * @param dbName The database name, which will be created if it does not
     * already exist.
     * @param jdbcUrl The URL used to connect to the database, e.g.
     * "jdbc:dbtype://host:port/" .
     * @param tableName The table name where data will be stored
     * @param properties Properties for the connection pool. Should include user
     * and password if appropriate.
     * @throws Exception If the data store cannot be initialized.
     */
    public JdbcDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        this.tableName = tableName;
        log.info("Connecting to mysql data table url={} db={} table={} ", jdbcUrl, dbName, tableName);
        // Verify the jdbcUrl and dbName, and create the database if it does not exist
        if (jdbcUrl == null || dbName == null || !jdbcUrl.endsWith("/")) {
            throw new IllegalArgumentException("jdbcUrl and dbName must be non-null, and jdbcUrl must end in '/'");
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            // Create a connection that excludes the database from the jdbc url.
            // This is necessary to create the database in the event that it does not exist.
            String dbSetupCommand = String.format("CREATE DATABASE IF NOT EXISTS %s", dbName);
            connection.prepareStatement(dbSetupCommand).execute();
        }
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(getDriverClass());
        cpds.setJdbcUrl(jdbcUrl + dbName);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setAcquireRetryDelay(acquireDelay);
        cpds.setAcquireRetryAttempts(acquireRetries);
        cpds.setMaxConnectionAge(maxConnectionAge);
        cpds.setProperties(properties);
        // Next create the data table within the database.
        runSetupTableCommand();
    }

    private static ResultSet executeAndTimeQuery(PreparedStatement preparedStatement) throws SQLException {
        TimerContext timerContext = queryTimer.time();
        int remainingRetries = insertRetries;
        while (remainingRetries > 0) {
            try {
                return preparedStatement.executeQuery();
            } catch (SQLTransactionRollbackException tre) {
                remainingRetries--;
                continue;
            } catch (SQLException e) {
                errorCounter.inc();
                throw e;
            } finally {
                timerContext.stop();
            }
        }
        throw new SQLException("Failed insert after retries");
    }

    private static boolean executeAndTimeInsert(PreparedStatement preparedStatement) throws SQLException {
        TimerContext timerContext = insertTimer.time();
        try {
            return preparedStatement.execute();
        } catch (SQLException e) {
            errorCounter.inc();
            throw e;
        } finally {
            timerContext.stop();
        }
    }

    /**
     * Do basic sanity checking for a path or childId value before operating on
     * the database.
     *
     * @param key The key to check
     */
    protected static void checkValidKey(String key) {
        if (key == null || blankChildValue.equals(key) || key.length() > maxPathLength) {
            throw new IllegalArgumentException("Invalid row key " + key + ": must be non-null and fewer than " + maxPathLength + " characters and not internal value " + blankChildValue);
        }
    }

    protected static void checkValidKeys(String key1, String key2) {
        checkValidKey(key1);
        checkValidKey(key2);
    }

    @Override
    public String get(String path) {
        checkValidKey(path);
        try {
            return querySingleResult(path, blankChildValue);
        } catch (NullPointerException npe) {
            /* Under some conditions a value set to null can cause an NPE in drizzle. This is a workaround. */
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Query the values of multiple rows using a single query, mandating that
     * childId=blankChildValue to ensure that no children are returned. Use
     * 'WHERE path IN (s1, s2, ...)' syntax, which performs better than a series
     * of ORs
     */
    @Override
    public Map<String, String> get(String[] paths) {
        if (paths == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder().append(String.format("SELECT %s,%s FROM %s WHERE %s=? AND %s IN (?", pathKey, valueKey, tableName, childKey, pathKey));
        for (int i = 0; i < paths.length - 1; i++) {
            sb.append(",?");
        }
        String command = sb.append(")").toString();
        Map<String, String> rv = new HashMap<>();
        try (Connection conn = cpds.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(command);
            // The first condition is that the child value is blankChildValue
            preparedStatement.setString(1, blankChildValue);
            int j = 2;
            for (String path : paths) {
                // The other condition is that the path key is in input set
                checkValidKey(path);
                preparedStatement.setString(j++, path);
            }
            ResultSet resultSet = executeAndTimeQuery(preparedStatement);
            while (resultSet.next()) {
                rv.put(resultSet.getString(pathKey), blobToValue(resultSet.getBlob(valueKey)));
            }
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }
        return rv;
    }

    protected Blob valueToBlob(String value) throws SQLException {
        return value != null ? new SerialBlob(LZFEncoder.encode(value.getBytes())) : null;
    }

    protected String blobToValue(Blob blob) throws SQLException, LZFException {
        return blob != null ? new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))) : null;
    }

    @Override
    public void put(String path, String value) throws Exception {
        checkValidKey(path);
        insert(path, blankChildValue, value);
    }

    /**
     * Internal method that uses the 'replace' mysql command to insert the row
     * if it is new, update otherwise.
     *
     * @param path The path to update
     * @param childId The childId to modify
     * @param value The value to insert
     * @throws SQLException If the command fails
     */
    private void insert(String path, String childId, String value) throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getInsertTemplate());
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId);
            executeAndTimeInsert(preparedStatement);
        }
    }

    /**
     * Query the value for a particular path/childId combination. Return null if
     * no row is found.
     *
     * @param path The path to query
     * @param childId The child to query
     * @return A String if a row was found; null otherwise. If multiple rows are
     * found, throw a RuntimeException
     * @throws SQLException
     */
    private String querySingleResult(String path, String childId) throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getQueryTemplate());
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, childId);
            ResultSet resultSet = executeAndTimeQuery(preparedStatement);
            String firstValue;
            boolean hasData = resultSet.next();
            if (!hasData) {
                return null;
            } else {
                firstValue = blobToValue(resultSet.getBlob(1));
            }
            boolean moreResults = resultSet.next();
            // Given the UNIQUE constraint, it would be extremely unexpected to find multiple values for a single path/childId
            if (moreResults) {
                throw new RuntimeException("Found multiple results after expecting a unique result; bailing");
            }
            return firstValue;
        } catch (LZFException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(String path, String childId) throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getDeleteTemplate());
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, childId);
            executeAndTimeInsert(preparedStatement);
        }
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        checkValidKeys(parent, childId);
        insert(parent, childId, value);
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        checkValidKeys(parent, childId);
        return querySingleResult(parent, childId);
    }

    @Override
    public void deleteChild(String parent, String childId) {
        checkValidKeys(parent, childId);
        try {
            delete(parent, childId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            checkValidKey(path);
            delete(path, blankChildValue);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    /**
     * Find the names of all children of a given path.
     */
    public List<String> getChildrenNames(String path) {
        checkValidKey(path);
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getGetChildNamesTemplate());
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildValue);
            ResultSet resultSet = executeAndTimeQuery(preparedStatement);
            boolean hasResults = resultSet.next();
            List<String> rv = new ArrayList<>();
            if (!hasResults) {
                return rv;
            }
            do {
                String key = resultSet.getString(1);
                if (key != null) {
                    rv.add(key);
                }
            } while (resultSet.next());
            return rv;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    /**
     * Get the names and values for each child of a given path.
     */
    public Map<String, String> getAllChildren(String path) {
        checkValidKey(path);
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getGetChildrenTemplate());
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildValue);
            ResultSet resultSet = executeAndTimeQuery(preparedStatement);
            boolean hasResults = resultSet.next();
            Map<String, String> rv = new HashMap<>();
            if (!hasResults) {
                return rv;
            }
            do {
                String key = resultSet.getString(1);
                String val = blobToValue(resultSet.getBlob(2));
                if (val != null) {
                    rv.put(key, val);
                }
            } while (resultSet.next());
            return rv;
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        cpds.close();
    }

    /**
     * On startup, create the table if it doesn't exist, and enforce that
     * path+childId combinations must be unique. Note: this is invoked from the
     * constructor, DO NOT use any variables that may not be initialized.
     *
     * @throws SQLException If creating execution fails
     */
    protected abstract void runSetupTableCommand() throws SQLException;

    /**
     * Gets the class to use for the JDBC Driver. Note: this is invoked from the
     * constructor, DO NOT use any variables that may not be initialized.
     *
     * @return
     */
    protected abstract String getDriverClass();

    protected abstract String getQueryTemplate();

    protected abstract String getInsertTemplate();

    protected abstract String getDeleteTemplate();

    protected abstract String getGetChildNamesTemplate();

    protected abstract String getGetChildrenTemplate();

    public static int getMaxPathLength() {
        return maxPathLength;
    }

    public static String getPathKey() {
        return pathKey;
    }

    public static String getValueKey() {
        return valueKey;
    }

    public static String getChildKey() {
        return childKey;
    }

    public static String getIdKey() {
        return idKey;
    }

}
