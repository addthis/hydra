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

import java.util.Properties;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlDataStore extends JdbcDataStore {
    private static final String description = "mysql";
    private final String insertTemplate;

    public MysqlDataStore(String host, int port, String dbName, String tableName, Properties properties) throws Exception {
        super("org.drizzle.jdbc.DrizzleDriver", "jdbc:mysql:thin://" + host + ":" + port + "/" + dbName, tableName, properties);
        if (host == null || dbName == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        runStartupCommand();
        insertTemplate = "insert into " + tableName +
                         "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                         "values( ? , ? , ? ) on duplicate key update " + valueKey + "=values(" + valueKey + ")";
    }

    @Override
    protected void runInsert(String path, String value, String childId) throws SQLException {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId != null ? childId : blankChildId);
            preparedStatement.execute();
            connection.commit();
        }

    }

    @Override
    protected Blob getValueBlobFromResultSet(ResultSet resultSet) throws SQLException {
        try {
            // Drizzle throws an NPE for the null blob.
            return resultSet.getBlob(valueKey);
        } catch (NullPointerException npe) {
            return null;
        }

    }
    @Override
    public String getDescription() {
        return description;
    }
}
