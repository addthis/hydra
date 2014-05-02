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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlInsertPickLastDataStore implements SpawnDataStore {
    private static final Logger log = LoggerFactory.getLogger(MysqlInsertPickLastDataStore.class);

    private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.drizzle.jdbc.DrizzleDriver");
    private static final String description = "mysqlinsertpicklast";
    private final ComboPooledDataSource cpds;
    /* The maximum allowable length for 'path' and 'child' values. */
    protected static final int maxPathLength = Parameter.intValue("sql.datastore.max.path.length", 200);
    /* Configuration parameters for the jdbc connection pool. */
    private static final int minPoolSize = Parameter.intValue("sql.datastore.minpoolsize", 10);
    private static final int maxPoolSize = Parameter.intValue("sql.datastore.maxpoolsize", 20);

    /* Column names. In the standard implementations, path and child are VARCHAR(200) and value is a BLOB. */
    protected static final String pathKey = "path";
    protected static final String valueKey = "val";
    protected static final String childKey = "child";
    protected static final String idKey = "id";
    /* The simulated 'child' value used to store data about the parent. */
    protected static final String blankChildId = "_root";
    /* Various command templates. Since they include the non-static table name, they cannot be static. */
    private final String queryTemplate;
    private final String insertTemplate;
    private final String deleteTemplate;
    private final String getChildNamesTemplate;
    private final String getChildrenTemplate;
    private final String tableName;

    /* Performance metrics */
    private static final Timer queryTimer = Metrics.newTimer(MysqlInsertPickLastDataStore.class, "mysqlQueryTime");
    private static final Timer insertTimer = Metrics.newTimer(MysqlInsertPickLastDataStore.class, "mysqlInsertTime");

    /**
     * Create the data pool, initialize the connection pool, and create the table if necessary.
      * @param jdbcUrl The URL used to connect to the database, e.g. "jdbc:mysql:thin://localhost:3306/spawndatabase"
     *                 It is assumed that this database has been created and basic privileges have been granted to the spawn user.
     * @param tableName The table name where data will be stored
     * @param properties Properties for the connection pool. Should include user and password if appropriate.
     * @throws Exception If the data store cannot be initialized.
     */
    public MysqlInsertPickLastDataStore(String jdbcUrl, String tableName, Properties properties) throws Exception {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setProperties(properties);
        this.tableName = tableName;
        log.info("Connecting to mysql data table url={} table={} ", jdbcUrl, tableName);
        runStartupCommand();
        /* Initialize templates. Done in constructor so they can be final */
        queryTemplate = String.format("SELECT %s FROM %s WHERE %s=? AND %s=?", valueKey, tableName, pathKey, childKey);
        insertTemplate = String.format("REPLACE INTO %s (%s,%s,%s) VALUES(?,?,?)", tableName, pathKey, valueKey, childKey);
        deleteTemplate = String.format("DELETE FROM %s WHERE %s=? AND %s=?", tableName, pathKey, childKey);
        getChildNamesTemplate = String.format("SELECT DISTINCT %s FROM %s WHERE %s=? AND %s!=?", childKey, tableName, pathKey, childKey);
        getChildrenTemplate = String.format("select %s,%s from %s b where %s=? and %s!=? and %s in (select max(%s) from %s b1 where b.%s = b1.%s group by %s)",
                childKey, valueKey, tableName, pathKey, childKey, idKey, idKey, tableName, childKey, childKey, childKey);
    }

    /**
     * Do basic sanity checking for a path or childId value before operating on the database.
     * @param key The key to check
     */
    private static void checkValidKey(String key) {
        if (key == null || blankChildId.equals(key) || key.length() > maxPathLength) {
            throw new IllegalArgumentException("Invalid row key " + key + ": must be non-null and fewer than " + maxPathLength + " characters and not internal value " + blankChildId);
        }
    }

    private static void checkValidKeys(String key1, String key2) {
        checkValidKey(key1);
        checkValidKey(key2);
    }

    /**
     * On startup, create the table if it doesn't exist, and enforce that path+childId combinations must be unique
      * @throws SQLException If creating execution fails
     */
    private void runStartupCommand() throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            String cmd = String.format("CREATE TABLE IF NOT EXISTS %s ( %s INT NOT NULL AUTO_INCREMENT, " +
                          "%s VARCHAR(%d) NOT NULL, %s MEDIUMBLOB, %s VARCHAR(%d), PRIMARY KEY (%s))",
                    tableName, idKey, pathKey, maxPathLength, valueKey, childKey, maxPathLength, idKey);

            connection.prepareStatement(cmd).execute();
            connection.prepareStatement("ALTER TABLE " + tableName + " ADD UNIQUE(" + pathKey + "," + childKey + ")").execute();
        }
    }

    private static ResultSet executeAndTimeQuery(PreparedStatement preparedStatement) throws SQLException {
        TimerContext timerContext = queryTimer.time();
        try {
            return preparedStatement.executeQuery();
        } finally {
            timerContext.stop();
        }
    }

    private static boolean executeAndTimeInsert(PreparedStatement preparedStatement) throws SQLException {
        TimerContext timerContext = insertTimer.time();
        try {
            return preparedStatement.execute();
        } finally {
            timerContext.stop();
        }
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String get(String path) {
        checkValidKey(path);
        try {
            return querySingleResult(path, blankChildId);
        } catch(NullPointerException npe) {
            /* Under some conditions a value set to null can cause an NPE in drizzle */
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Blob valueToBlob(String value) throws SQLException {
        return value != null ? new SerialBlob(LZFEncoder.encode(value.getBytes())) : null;
    }

    private static String blobToValue(Blob blob) throws SQLException, LZFException {
        return blob != null ? new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))) : null;
    }

    @Override
    /**
     * Query the values of multiple rows using a single query, mandating that childId=blankChildId to ensure that no children are returned.
     * Use 'WHERE path IN (s1, s2, ...)' syntax, which performs better than a series of ORs
     */
    public Map<String, String> get(String[] paths) {
        if (paths == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder().append(String.format("SELECT %s,%s FROM %s WHERE %s=? AND %s IN (?", pathKey, valueKey, tableName, childKey, pathKey));
        for (int i=0; i<paths.length-1; i++) {
            sb.append(",?");
        }
        String command = sb.append(")").toString();
        Map<String, String> rv = new HashMap<>();
        try (Connection conn = cpds.getConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(command);
            preparedStatement.setString(1, blankChildId);
            int j=2;
            for (String path : paths) {
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

    @Override
    public void put(String path, String value) throws Exception {
        checkValidKey(path);
        insert(path, blankChildId, value);
    }

    /**
     * Internal method that uses the 'replace' mysql command to insert the row if it is new, update otherwise.
     * @param path The path to update
     * @param childId The childId to modify
     * @param value The value to insert
     * @throws SQLException If the command fails
     */
    private void insert(String path, String childId, String value)  throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId);
            executeAndTimeInsert(preparedStatement);
        }
    }

    /**
     * Query the value for a particular path/childId combination. Return null if no row is found.
     * @param path The path to query
     * @param childId The child to query
     * @return A String if a row was found; null otherwise. If multiple rows are found, throw a RuntimeException
     * @throws SQLException
     */
    private String querySingleResult(String path, String childId) throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(queryTemplate);
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
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
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
            delete(path, blankChildId);
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
            PreparedStatement preparedStatement = connection.prepareStatement(getChildNamesTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
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
            PreparedStatement preparedStatement = connection.prepareStatement(getChildrenTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
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
}
