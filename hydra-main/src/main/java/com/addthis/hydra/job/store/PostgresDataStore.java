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
import java.sql.PreparedStatement;

import com.addthis.basis.util.Parameter;

/**
 * A class for storing spawn configuration data into a PostgreSQL database.
 * Reads and writes values from a single master table which uses partitioning
 * {@link http://www.postgresql.org/docs/9.1/static/ddl-partitioning.html}.
 */
public class PostgresDataStore extends JdbcDataStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresDataStore.class);

    private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.postgresql.Driver");

    public PostgresDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        super(jdbcUrl, dbName.toLowerCase(), tableName, properties);
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
            final String tableSetupCommand = new StringBuffer("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" ( ")
                            .append(getIdKey()).append(" SERIAL PRIMARY KEY, ") // Auto-incrementing int id
                            .append(getPathKey()).append(" VARCHAR(").append(getMaxPathLength()).append(") NOT NULL, ") // VARCHAR path
                            .append(getValueKey()).append(" VARCHAR, ")//VARCHAR value
                            .append(getChildKey()).append(" VARCHAR(").append(getMaxPathLength()).append("), ") // VARCHAR child
                            .append("CONSTRAINT parent_child UNIQUE (").append(getPathKey()).append(", ").append(getChildKey()).append(")) ") // enforce unique (path, child) combo
                    .toString();
            connection.prepareStatement(tableSetupCommand).execute();
            final String replaceFunctionSetupCommand = String.format(
                    "CREATE OR REPLACE FUNCTION replace_entry(%s VARCHAR, %s VARCHAR, %s VARCHAR) RETURNS void AS $$\n"
            + "BEGIN\n"
            + "        IF EXISTS( SELECT * FROM %s WHERE %s = %s ) THEN\n"
            + "                UPDATE %s\n"
            + "                SET %s = %s, %s = %s WHERE %s = %s;\n"
            + "        ELSE\n"
            + "                INSERT INTO %s(%s, %s, %s) VALUES( %s, %s, %s );\n"
            + "        END IF;\n"
            + "\n"
            + "        RETURN;\n"
            + "END;\n"
            + "$$ LANGUAGE plpgsql;",
                    getPathKey() + "Var", getValueKey() + "Var", getChildKey() + "Var",
                    tableName, getPathKey(), getPathKey() + "Var",
                    tableName,
                    getValueKey(), getValueKey() + "Var", getChildKey(), getChildKey() + "Var" ,getPathKey(), getPathKey() + "Var",
                    tableName, getPathKey(), getValueKey(), getChildKey(), getPathKey() + "Var", getValueKey() + "Var", getChildKey() + "Var");
            connection.prepareStatement(replaceFunctionSetupCommand).execute();
            try {
                final String createIndexCommand = String.format("CREATE INDEX ON %s (%s)", tableName, getPathKey());
                connection.prepareStatement(createIndexCommand).execute();
            } catch (SQLException sqle) {
                //do nothing
                log.debug("Could not create index", sqle);
                log.info("Could not create index - may be ok if it already exists");
            }
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
    protected PreparedStatement getInsertTemplate(Connection connection, String path, String childId, String value) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement("select * from replace_entry(?,?,?)");
        preparedStatement.setString(1, path);
        preparedStatement.setString(2, value);
        preparedStatement.setString(3, childId);
        return preparedStatement;
    }
    
    @Override
    protected String getDeleteTemplate() {
        return String.format("DELETE FROM %s WHERE %s=? AND %s=?", tableName, getPathKey(), getChildKey());
    }

    @Override
    protected String getGetChildNamesTemplate() {
        return String.format("SELECT DISTINCT %s FROM %s WHERE %s=? AND %s!=?", getChildKey(), tableName, getPathKey(), getChildKey());
    }

    @Override
    protected String getGetChildrenTemplate() {
        return String.format("SELECT %s,%s FROM %s WHERE %s=? AND %s!=?",
                getChildKey(), getValueKey(), tableName, getPathKey(), getChildKey());
    }

    @Override
    public String getDescription() {
        return "Postgres";
    }

    @Override
    protected Class getValueType() {
        return String.class;
    }
    
    @Override
    protected String valueToDBType(String value) throws SQLException {
        return value;
    }

    @Override
    protected <T> String dbTypeToValue(T dbValue) throws SQLException {
        return (String) dbValue;
    }

}
