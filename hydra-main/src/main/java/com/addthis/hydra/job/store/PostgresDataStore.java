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

import com.addthis.basis.util.Parameter;

/**
 * A class for storing spawn configuration data into a PostgreSQL database. 
 * Reads and writes values from a single master table which uses partitioning {@link http://www.postgresql.org/docs/9.1/static/ddl-partitioning.html}.
 */
public class PostgresDataStore extends JdbcDataStore {
    
   private static final Logger log = LoggerFactory.getLogger(PostgresDataStore.class);
   
   private static final String driverClass = Parameter.value("sql.datastore.driverclass", "org.postgresql.Driver");

    public PostgresDataStore(String jdbcUrl, String dbName, String tableName, Properties properties) throws Exception {
        super(jdbcUrl, dbName, tableName, properties);
        log.info("Postgres Spawn Data Store Initialized");
    }

    @Override
    protected void runSetupTableCommand() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String getDriverClass() {
        return driverClass;
    }

    @Override
    protected String getQueryTemplate() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String getInsertTemplate() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
   
}
