package com.addthis.hydra.job.store;

import javax.sql.rowset.serial.SerialBlob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

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

import org.slf4j.Logger;

public class MysqlInsertPickLastDataStore implements SpawnDataStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MysqlInsertPickLastDataStore.class);

    private static final String description = "mysqlinsertpicklast";
    private final ComboPooledDataSource cpds;
    /* The maximum allowable length for 'path' and 'child' values. */
    protected static final int maxPathLength = Parameter.intValue("sql.datastore.max.path.length", 200);
    /* Configuration parameters for the jdbc connection pool. */
    private static final int minPoolSize = Parameter.intValue("sql.datastore.minpoolsize", 10);
    private static final int maxPoolSize = Parameter.intValue("sql.datastore.maxpoolsize", 20);
    /* How frequently to clean up duplicate values for the same key */
    private static final int cleanupFrequencySeconds = Parameter.intValue("sql.datastore.cleanupseconds", 30);

    /* Column names. In the standard implementations, path and child are VARCHAR(200) and value is a BLOB or TEXT. */
    protected static final String pathKey = "path";
    protected static final String valueKey = "val";
    protected static final String childKey = "child";
    protected static final String idKey = "id";
    /* The simulated 'child' value used to store data about the parent. */
    protected static final String blankChildId = "_root";
    private final String queryTemplate;
    private final String insertTemplate;
    private final String deleteTemplate;
    private final String getChildNamesTemplate;
    private final String getChildrenTemplate;
    private final String tableName;
    private final String cleanupTemplate;

    public MysqlInsertPickLastDataStore(String jdbcUrl, String tableName, Properties properties, boolean autoCleanup) throws Exception {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("org.drizzle.jdbc.DrizzleDriver");
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMinPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setProperties(properties);
        this.tableName = tableName;
        runStartupCommand();
        queryTemplate = String.format("SELECT %s FROM %s WHERE %s=? AND %s=? ORDER BY %s DESC LIMIT 1", valueKey, tableName, pathKey, childKey, idKey);
        insertTemplate = String.format("INSERT INTO %s (%s,%s,%s) VALUES(?,?,?)", tableName, pathKey, valueKey, childKey);
        deleteTemplate = String.format("DELETE FROM %s WHERE %s=? AND %s=?", tableName, pathKey, childKey);
        getChildNamesTemplate = String.format("SELECT DISTINCT %s FROM %s WHERE %s=? AND %s!=?", childKey, tableName, pathKey, childKey);
        getChildrenTemplate = "SELECT " + childKey +"," + valueKey  + " FROM " + tableName + " WHERE " + pathKey + "=? AND " + childKey + "!=? ORDER BY " + idKey; // SUPER HACKY. BASICALLY SO THE PUT COMMANDS WILL PUT THE MOST UP-TO-DATE THING IN LAST. THIS BLOWS!!!!!!!!
        cleanupTemplate = String.format("DELETE v FROM %s AS v INNER JOIN %s AS v2 ON (v.%s,v.%s) = (v2.%s,v2.%s) AND v.%s < v2.%s", tableName, tableName, pathKey, childKey, pathKey, childKey, idKey, idKey); // v and v2 are dummy values; basically, delete all but the latest value for any (parent, child) combination. Not necessary for correctness, but should be run periodically to clean up data storage.
        if (autoCleanup) {
            new Timer("mysqldatastore_cleanup").scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        cleanUp();
                    } catch (SQLException e) {
                        log.warn("Failed to perform mysql cleanup", e);
                    }
                }
            }, cleanupFrequencySeconds, cleanupFrequencySeconds);

        }

    }

    public void cleanUp() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.prepareStatement(cleanupTemplate).execute();
        }
    }

    public int countRows() throws SQLException {
        try (Connection connection = getConnection()) {
            ResultSet resultSet = connection.prepareStatement("SELECT COUNT(*) FROM " + tableName).executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private void runStartupCommand() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                        + idKey + " MEDIUMINT NOT NULL AUTO_INCREMENT, "
                                        + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                        + valueKey + " MEDIUMBLOB, "
                                        + childKey + " VARCHAR(" + maxPathLength + "), "
                                        + "PRIMARY KEY (" + idKey + "))"
            ).execute();
        }
    }

    protected Connection getConnection() throws SQLException {
        return cpds.getConnection();
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String get(String path) {
        try {
            return query(path, blankChildId);
        } catch(NullPointerException npe ) {
            return null;
        } catch (SQLException e) {
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
    public Map<String, String> get(String[] paths) {
        // WHERE PATH=? OR PATH=? OR PATH=? IS FASTER
        Map<String, String> rv = new HashMap<>();
        for (String path : paths) {
            String val = get(path);
            if (val != null) {
                rv.put(path, val);
            }
        }
        return rv;
    }

    @Override
    public void put(String path, String value) throws Exception {
        insert(path, blankChildId, value);
    }

    private void insert(String path, String childId, String value)  throws SQLException {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId);
            preparedStatement.execute();
        }
    }

    private String query(String path, String childId) throws SQLException {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(queryTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, childId);
            ResultSet resultSet = preparedStatement.executeQuery();
            boolean hasData = resultSet.next();
            if (!hasData) {
                return null;
            } else {
                return blobToValue(resultSet.getBlob(1));
            }
        } catch (LZFException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(String path, String childId) throws SQLException {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, childId);
            preparedStatement.execute();
        }
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        insert(parent, childId, value);
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        return query(parent, childId);
    }

    @Override
    public void deleteChild(String parent, String childId) {
        try {
            delete(parent, childId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            delete(path, blankChildId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getChildrenNames(String path) {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getChildNamesTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
            ResultSet resultSet = preparedStatement.executeQuery();
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
    public Map<String, String> getAllChildren(String path) {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(getChildrenTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setString(2, blankChildId);
            ResultSet resultSet = preparedStatement.executeQuery();
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
