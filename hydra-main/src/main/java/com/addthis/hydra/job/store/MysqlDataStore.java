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

import java.sql.Connection;
import java.sql.SQLException;

import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A class for storing spawn configuration data into a mysql database. Reads and writes values from a single table.
 */
public class MysqlDataStore extends JdbcDataStore {
    private static final Logger log = LoggerFactory.getLogger(MysqlDataStore.class);
    private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.drizzle.jdbc.DrizzleDriver");
    
    /* There are known issues with Drizzle and InnoDB tables. Using the MyISAM type is strongly recommended. */
    private static final String tableType = Parameter.value("sql.datastore.tabletype", "MyISAM");
    private static final String description = "mysql";

    public MysqlDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        super(jdbcUrl, dbName, tableName, properties);
        log.info("Mysql Spawn Data Store Initialized");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runSetupTableCommand() throws SQLException {
        try (Connection connection = cpds.getConnection()) {
            String tableSetupCommand = String.format("CREATE TABLE IF NOT EXISTS %s ( " +
                                       "%s INT NOT NULL AUTO_INCREMENT, " + // Auto-incrementing int id
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
    protected String getInsertTemplate() {
        //TODO - cache this
        return String.format("REPLACE INTO %s (%s,%s,%s) VALUES(?,?,?)", tableName, getPathKey(), getValueKey(), getChildKey());
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
}
