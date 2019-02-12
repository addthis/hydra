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

import java.util.Properties;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.addthis.basis.util.Parameter;

import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing spawn configuration data into a mysql database. Reads and
 * writes values from a single table.
 */
public class MysqlDataStore extends JdbcDataStore<Blob> {

    private static final Logger log = LoggerFactory.getLogger(MysqlDataStore.class);
    private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.drizzle.jdbc.DrizzleDriver");

    /* There are known issues with Drizzle and InnoDB tables. Using the MyISAM type is strongly recommended. */
    private static final String tableType = Parameter.value("sql.datastore.tabletype", "MyISAM");
    private static final String description = "mysql";

    public MysqlDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        super(jdbcUrl, dbName, tableName, properties);
        log.info("Mysql Spawn Data Store Initialized");
    }

    @Override
    protected void runSetupDatabaseCommand(final String dbName, final String jdbcUrl, final Properties properties) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            // Create a connection that excludes the database from the jdbc url.
            // This is necessary to create the database in the event that it does not exist.
            String dbSetupCommand = String.format("CREATE DATABASE IF NOT EXISTS %s", dbName);
            connection.prepareStatement(dbSetupCommand).execute();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runSetupTableCommand() throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            String tableSetupCommand = String.format("CREATE TABLE IF NOT EXISTS %s ( "
                    + "%s INT NOT NULL AUTO_INCREMENT, " + // Auto-incrementing int id
                    "%s VARCHAR(%d) NOT NULL, %s MEDIUMBLOB, %s VARCHAR(%d), " + // VARCHAR path, BLOB value, VARCHAR child
                    "PRIMARY KEY (%s), UNIQUE KEY (%s,%s)) " + // Use id as primary key, enforce unique (path, child) combo
                    "ENGINE=%s", // Use specified table type (MyISAM works best in practice)
                    tableName, getIdKey(), getPathKey(), getMaxPathLength(), getValueKey(), getChildKey(),
                    getMaxPathLength(), getIdKey(), getPathKey(), getChildKey(), tableType);
            connection.prepareStatement(tableSetupCommand).execute();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getQueryTemplate() {
        //TODO - cache this
        return String.format("SELECT %s FROM %s WHERE %s=? AND %s=?", getValueKey(), tableName, getPathKey(), getChildKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PreparedStatement getInsertTemplate(Connection connection, String path, String childId, String value) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(
                String.format("REPLACE INTO %s (%s,%s,%s) VALUES(?,?,?)", tableName, getPathKey(), getValueKey(), getChildKey()));
        preparedStatement.setString(1, path);
        preparedStatement.setBlob(2, valueToDBType(value));
        preparedStatement.setString(3, childId);
        return preparedStatement;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDeleteTemplate() {
        //TODO - cache this
        return String.format("DELETE FROM %s WHERE %s=? AND %s=?", tableName, getPathKey(), getChildKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getGetChildNamesTemplate() {
        //TODO - cache this
        return String.format("SELECT DISTINCT %s FROM %s WHERE %s=? AND %s!=?", getChildKey(), tableName, getPathKey(), getChildKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getGetChildrenTemplate() {
        return String.format("SELECT %s,%s FROM %s WHERE %s=? AND %s!=?",
                getChildKey(), getValueKey(), tableName, getPathKey(), getChildKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDriverClass() {
        return driverClass;
    }

    @Override
    protected Class<Blob> getValueType() {
        return Blob.class;
    }

    @Override
    protected Blob valueToDBType(String value) throws SQLException {
        if (value != null) {
            return new SerialBlob(value.getBytes(StandardCharsets.UTF_8));
        } else {
            return null;
        }
    }

    @Override
    protected String dbTypeToValue(Blob dbValue) throws SQLException {
        try {
            if (dbValue != null) {
                byte[] blobBytes = dbValue.getBytes(1L, (int) dbValue.length());
                // make best guess whether value was previously compressed with LZF
                if ((blobBytes.length > 0) && (blobBytes[0] == LZFChunk.BYTE_Z) && (blobBytes[1] == LZFChunk.BYTE_V)) {
                    byte[] decodedBytes = LZFDecoder.decode(blobBytes);
                    return new String(decodedBytes, StandardCharsets.UTF_8);
                } else {
                    return new String(blobBytes, StandardCharsets.UTF_8);
                }
            } else {
                return null;
            }
        } catch (LZFException ex) {
            //Couldn't deal with the data
            throw new SQLException(ex);
        }
    }

}
