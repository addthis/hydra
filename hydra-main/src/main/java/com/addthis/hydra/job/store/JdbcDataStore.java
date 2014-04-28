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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.CodecJSON;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;

/**
 * An abstract class for storing spawn data in a jdbc-compatible database. Implementations differ primarily in their
 * data table setup and in their implementation of the 'upsert' operation.
 */
public abstract class JdbcDataStore implements SpawnDataStore {
    private static final CodecJSON codecJSON = new CodecJSON();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcDataStore.class);

    /* Column names. In the standard implementations, path and child are VARCHAR(200) and value is a BLOB or TEXT. */
    protected static final String pathKey = "path";
    protected static final String valueKey = "val";
    protected static final String childKey = "child";
    /* The simulated 'child' value used to store data about the parent. */
    protected static final String blankChildId = "_root";

    /* The maximum allowable length for 'path' and 'child' values. */
    protected static final int maxPathLength = Parameter.intValue("sql.datastore.max.path.length", 200);
    /* Configuration parameters for the jdbc connection pool. */
    private static final int minPoolSize = Parameter.intValue("sql.datastore.minpoolsize", 10);
    private static final int maxPoolSize = Parameter.intValue("sql.datastore.maxpoolsize", 20);

    private static final Timer insertTimer = Metrics.newTimer(JdbcDataStore.class, "insertTime");
    private static final Timer getTimer = Metrics.newTimer(JdbcDataStore.class, "getTime");

    protected final String tableName;
    private final ComboPooledDataSource cpds;

    /**
     * Create the SpawnDataStore and create the jdbc connection pool.
     * @param driverClass The driver class to load, e.g. org.someorg.jdbc.Driver
     * @param jdbcUrl The jdbc connection URL.
     * @param tableName The table name to use. Must be a legal name for the database being used.
     * @param properties The Properties for the jdbc connection. Set username and password here if necessary.
     * @throws Exception If the jdbc configuration values are not valid
     */
    public JdbcDataStore(String driverClass, String jdbcUrl, String tableName, Properties properties) throws Exception {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setProperties(properties);
        this.tableName = tableName;
    }

    protected Connection getConnection() throws SQLException {
        Connection conn = cpds.getConnection();
        conn.setAutoCommit(this.useAutoCommit());
        return conn;
    }

    /**
     * Run the startup command after setup is complete. Creates the table if it doesn't already exist.
     * @throws SQLException
     */
    protected void runStartupCommand() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                         + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                         + valueKey + " MEDIUMBLOB, "
                                         + childKey + " VARCHAR(" + maxPathLength + "), "
                                         + "PRIMARY KEY (" + pathKey + ", " + childKey + "))"
            ).execute();
            if (!useAutoCommit()) {
                connection.commit();
            }
        }
    }

    /**
     * Run the insert command. Should be performed as an 'upsert' operation -- create the row if this combination of
     * path and childId is already in the database; otherwise, modify the value of the row. Implementation varies among
     * database types.
     * @param path The path being modified
     * @param value The value to be inserted
     * @param childId The particular child for path being modified
     * @throws SQLException
     */
    protected abstract void runInsert(String path, String value, String childId) throws SQLException;

    protected abstract boolean useAutoCommit();

    private ResultSet executeAndTimeQuery(PreparedStatement preparedStatement) throws SQLException {
        TimerContext timerContext = getTimer.time();
        try {
            return preparedStatement.executeQuery();
        } finally {
            timerContext.stop();
        }
    }

    @Override
    public String get(String path) {
        try (Connection connection = getConnection()){
            PreparedStatement preparedStatement = connection.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey + "=? and " + childKey + "=?");
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
            return getSingleResult(executeAndTimeQuery(preparedStatement));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateMultiQuery(String[] paths) {
        StringBuilder sb = new StringBuilder();
        sb.append("select " + (pathKey + "," + valueKey) + " from " + tableName);
        boolean started = false;
        for (int i=0; i<paths.length; i++) {
            sb.append((started ? " or " : " where ") + pathKey + "=?");
            started = true;
        }
        sb.append(" and " + childKey + "=?");
        return sb.toString();
    }

    @Override
    public Map<String, String> get(String[] paths) {
        if (paths == null) {
            return null;
        }
        Map<String, String> rv = new HashMap<>();
        String query = generateMultiQuery(paths);
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            int j=1;
            for (String path : paths) {
                preparedStatement.setString(j++, path);
            }
            preparedStatement.setString(j, blankChildId);
            ResultSet resultSet = executeAndTimeQuery(preparedStatement);
            boolean foundRows = resultSet.next();
            if (!foundRows) {
                return null;
            }
            do {
                Blob blob = getValueBlobFromResultSet(resultSet);
                if (blob != null) {
                    rv.put(resultSet.getString(pathKey), blobToValue(blob));
                }
            } while(resultSet.next());
            return rv;
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }
    }

    private void insert(String path, String value, String childId) throws SQLException {
        if (path.length() > maxPathLength || (childId != null && childId.length() > maxPathLength)) {
            throw new IllegalArgumentException("Input path longer than max of " + maxPathLength);
        }
        TimerContext timerContext = insertTimer.time();
        try {
            runInsert(path, value, childId);
        } finally {
            timerContext.stop();
        }
    }

    @Override
    public void put(String path, String value) throws Exception {
        insert(path, value, null);
    }

    /**
     * Do basic validity testing on a childId value. Throw an IllegalArgumentException if it is invalid.
     * @param childId The childId to check.
     */
    private static void checkValidChildId(String childId) {
        if (blankChildId.equals(childId)) {
            throw new IllegalArgumentException("Child id " + blankChildId + " is reserved for internal usage");
        }
        if (childId == null || childId.length() > maxPathLength) {
            throw new IllegalArgumentException("Invalid child id " + childId + ": must be non-null and fewer than " + maxPathLength + " characters");
        }
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        checkValidChildId(childId);
        insert(parent, value, childId);
    }

    protected Blob getValueBlobFromResultSet(ResultSet resultSet) throws SQLException {
        // Needs to be overwritten in MysqlDataStore to get around an annoying drizzle bug
        return resultSet.getBlob(valueKey);
    }

    /**
     * Extract the single value entry from a ResultSet where at most one entry is expected.
     * @param resultSet The ResultSet to check.
     * @return The String value if a single result is found; null if none are found. If more than one value is found, throw a RuntimeException
     * @throws SQLException If there is a failure iterating through the ResultSet
     */
    private String getSingleResult(ResultSet resultSet) throws SQLException {
        boolean foundRows = resultSet.next();
        if (!foundRows) {
            return null;
        }
        Blob b = getValueBlobFromResultSet(resultSet);
        String firstResult = null;
        if (b != null) {
            try {
                firstResult = blobToValue(b);
            } catch (LZFException e) {
                throw new RuntimeException(e);
            }
        }
        boolean moreResults = resultSet.next();
        if (moreResults) {
            throw new RuntimeException("Found multiple results after expecting a unique result; bailing");
        }
        resultSet.close();
        return firstResult;
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        checkValidChildId(childId);
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey + "=? and " + childKey + "=?");
            preparedStatement.setString(1, parent);
            preparedStatement.setString(2, childId);
            return getSingleResult(executeAndTimeQuery(preparedStatement));
        }
    }

    @Override
    public void deleteChild(String parent, String childId) {
        checkValidChildId(childId);
        try (Connection connection = getConnection()) {
            String deleteTemplate = "delete from " + tableName + " where " + pathKey + "=? and " + childKey + "=?";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, parent);
            preparedStatement.setString(2, childId);
            preparedStatement.execute();
            if (!useAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try (Connection connection = getConnection()){
            String deleteTemplate = "delete from " + tableName + " where " + pathKey + "=?";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.execute();
            if (!useAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected ResultSet getResultsForQuery(PreparedStatement preparedStatement) throws SQLException {
        ResultSet resultSet = executeAndTimeQuery(preparedStatement);
        boolean foundRows = resultSet.next();
        if (!foundRows) {
            return null;
        }
        return resultSet;

    }

    @Override
    public List<String> getChildrenNames(String path) {
        try (Connection connection = getConnection()){
            String template =  "select " + childKey + " from " + tableName + " where " + pathKey + "=? and " + childKey + "!=?";
            PreparedStatement preparedStatement = connection.prepareStatement(template);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
            ResultSet resultSet = getResultsForQuery(preparedStatement);
            ArrayList<String> rv = new ArrayList<>();
            if (resultSet == null) {
                return rv;
            }
            do {
                rv.add(resultSet.getString(1));
            } while (resultSet.next());
            return rv;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        String template = "select " + childKey + "," + valueKey + " from " + tableName + " where " + pathKey + "=? and " + childKey + "!=?";
        try (Connection connection = getConnection()) {
            HashMap<String, String> rv = new HashMap<>();
            PreparedStatement preparedStatement = connection.prepareStatement(template);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
            ResultSet resultSet = getResultsForQuery(preparedStatement);
            if (resultSet == null) {
                return rv;
            }
            do {
                Blob blob = resultSet.getBlob(2);
                if (blob != null) {
                    rv.put(resultSet.getString(1), blobToValue(blob));
                }
            } while (resultSet.next());
            return rv;
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }

    }

    protected static Blob valueToBlob(String value) throws SQLException {
        return value != null ? new SerialBlob(LZFEncoder.encode(value.getBytes())) : null;
    }

    protected static String blobToValue(Blob blob) throws SQLException, LZFException {
        return blob != null ? new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))) : null;
    }

    @Override
    public void close() {
        cpds.close();
    }
}
