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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;

import com.addthis.basis.util.Parameter;

import static com.addthis.hydra.job.store.JdbcDataStore.getChildKey;
import static com.addthis.hydra.job.store.JdbcDataStore.getIdKey;
import static com.addthis.hydra.job.store.JdbcDataStore.getMaxPathLength;
import static com.addthis.hydra.job.store.JdbcDataStore.getPathKey;
import static com.addthis.hydra.job.store.JdbcDataStore.getValueKey;

/**
 * A class for storing spawn configuration data into a PostgreSQL database.
 * Reads and writes values from a single master table which uses partitioning
 * {@link http://www.postgresql.org/docs/9.1/static/ddl-partitioning.html}.
 */
public class PostgresDataStore extends JdbcDataStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresDataStore.class);

    private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.postgresql.Driver");

    public PostgresDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        super(jdbcUrl, dbName, tableName, properties);
        log.info("Postgres Spawn Data Store Initialized");
    }
    
    @Override
    protected void runSetupDatabaseCommand(String dbName, String jdbcUrl, Properties properties) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
            // Create a connection that excludes the database from the jdbc url.
            // This is necessary to create the database in the event that it does not exist.
            String dbSetupCommand = String.format("CREATE DATABASE %s", dbName);
            connection.prepareStatement(dbSetupCommand).execute();
        } catch (final SQLException se) {
            //the database may already exists
            if (se.getMessage().endsWith("already exists")) {
                log.info("Database already exists");
            } else {
                throw se;
            }
        }
    }

    @Override
    protected void runSetupTableCommand() throws SQLException {
        try (final Connection connection = cpds.getConnection()) {
            String tableSetupCommand = String.format("CREATE TABLE IF NOT EXISTS %s ( " +
                                       "%s SERIAL PRIMARY KEY, " + // Auto-incrementing int id
                                       "%s VARCHAR(%d) NOT NULL, %s VARCHAR, %s VARCHAR(%d), " + // VARCHAR path, VARCHAR value, VARCHAR child
                                       "CONSTRAINT parent_child UNIQUE (%s,%s)) " + // Use id as primary key, enforce unique (path, child) combo
                    tableName, 
                    getIdKey(), 
                    getPathKey(), getMaxPathLength(), getValueKey(), getChildKey(), getMaxPathLength(),
                    getPathKey(), getChildKey());
            connection.prepareStatement(tableSetupCommand).execute();
        }
    }

    @Override
    protected String getDriverClass() {
        return driverClass;
    }

    @Override
    protected String getQueryTemplate() {
        return String.format("SELECT %s FROM %s WHERE %s=? AND %s=?", getValueKey(), tableName, getPathKey(), getChildKey());
    }

    @Override
    protected String getInsertTemplate() {
        return String.format("REPLACE INTO %s (%s,%s,%s) VALUES(?,?,?)", tableName, getPathKey(), getValueKey(), getChildKey());
    }

    @Override
    protected String getDeleteTemplate() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String getGetChildNamesTemplate() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String getGetChildrenTemplate() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getDescription() {
        return "Postgres";
    }

}
