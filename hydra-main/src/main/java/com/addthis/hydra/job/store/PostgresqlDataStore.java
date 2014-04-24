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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresqlDataStore extends JdbcDataStore {

    private static final String description = "postgres";
    private final String insertTemplate;

    public PostgresqlDataStore(String host, int port, String dbName, String tableName, Properties properties) throws Exception {
        super("org.postgresql.Driver", "jdbc:postgresql://" + host + ":" + port + "/" + dbName, tableName, properties);
        if (host == null || dbName == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        // Per http://stackoverflow.com/a/8702291 and performance testing, this is the best way to do upsert in postgres
        insertTemplate = "WITH new_values (" + pathKey + ", " + valueKey + ", " + childKey + ") as (values (?, ?, ?)),\n" +
                         "upsert as( \n" +
                         "    update " + tableName + " m \n" +
                         "        set " + valueKey + " = nv." + valueKey + "\n" +
                         "    FROM new_values nv\n" +
                         "    WHERE m." + pathKey + " = nv." + pathKey  + " AND m." + childKey + " = nv." + childKey + "\n" +
                         "    RETURNING m.*\n" +
                         ")\n" +
                         "INSERT INTO "+ tableName +  " (" + pathKey + ", " + valueKey + ", " + childKey + ")\n" +
                         "SELECT " + pathKey + ", " + valueKey + ", " + childKey + " FROM new_values\n" +
                         "WHERE NOT EXISTS (SELECT 1 \n" +
                         "                  FROM upsert up \n" +
                         "                  WHERE up." + pathKey + " = new_values." + pathKey +" AND up." + childKey + " = new_values." + childKey + ")";
        runStartupCommand();

    }

    @Override
    protected void runStartupCommand() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                        + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                        + valueKey + " TEXT, " // Postgres does not recognize the 'blob' type
                                        + childKey + " VARCHAR(" + maxPathLength + "), "
                                        + "PRIMARY KEY (" + pathKey + ", " + childKey + "))"
            ).execute();
            connection.commit();
        }
    }

    @Override
    protected void runInsert(String path, String value, String childId) throws SQLException {
        childId = childId != null ? childId : blankChildId;
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId);
            preparedStatement.execute();
            connection.commit();
        }
    }

    @Override
    protected boolean useAutoCommit() {
        // Postgres does not support using autocommit with BLOB objects.
        return false;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
