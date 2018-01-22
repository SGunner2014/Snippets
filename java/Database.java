package com.samgunner.general;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Represents a connection to the database and contains functions to interact with the database
 */
public class Database {
    private Connection conn;

    /**
     * Instantiates the class and creates the database connection
     */
    public Database() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser("");
        dataSource.setPassword("");
        dataSource.setDatabaseName("");
        dataSource.setServerName("");

        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inserts a record into the database and returns the ID of the new record
     * @param fields The fields to insert into the database
     * @param data The data to insert along with the fields
     * @return The ID of the new record
     */
    public int insert(String table, ArrayList<String> fields, ArrayList<String> data) throws SQLException {
        Statement statement = conn.createStatement();
        String toExecute = "INSERT INTO " + table + " (";

        //Loop through the fields and add them to the query
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0) {
                toExecute += fields.get(i);
            } else {
                toExecute += ", " + fields.get(i);
            }
        }
        toExecute += ") VALUES (";

        //Loop through the values and add them to the query
        for (int i = 0; i < data.size(); i++) {
            if (i == 0) {
                toExecute += "'" + data.get(i) + "'";
            } else {
                toExecute += ", " + "'" + data.get(i) + "'";
            }
        }
        toExecute += ")";

        int rowsUpdated = statement.executeUpdate(toExecute);

        //Get the id of the row created
        ResultSet generatedKeys = statement.getGeneratedKeys();
        if (generatedKeys.next()) {
            return generatedKeys.getInt(1);
        } else {
            throw new SQLException("Creating user failed, no ID obtained.");
        }
    }

    /**
     * Selects a number of records from a table
     * @param table The table to select the data from
     * @param fields The fields that should be satisfied
     * @param data The data that should be satisfied
     * @return The data returned by the query
     * @throws SQLException Thrown if query not built correctly
     */
    public ResultSet select(String table, ArrayList<String> fields, ArrayList<String> data) throws SQLException {
        Statement statement = conn.createStatement();
        String toExecute = "SELECT * FROM " + table + " WHERE ";
        String current;

        //Loop through the fields and data and build up the query to execute
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0) {
                current = fields.get(i) + " = '" + data.get(i) + "' ";
            } else {
                current = "AND " + fields.get(i) + " = '" + data.get(i) + "' ";
            }
            toExecute += current;
        }

        //Return the results
        return statement.executeQuery(toExecute);
    }

    /**
     * Returns all the records within a specified table
     * @param table The table to select the results from
     * @return The results obtained from the table
     */
    public ResultSet selectAll(String table) throws SQLException {
        Statement statement = conn.createStatement();
        String toExecute = "SELECT * FROM " + table;

        return statement.executeQuery(toExecute);
    }

    /**
     * Updates a number of records in a table if they satisfy a specified condition
     * @param table The table name within which the records should be updated
     * @param fields The fields that should be satisfied within the table
     * @param data The data that should be satisfied within the table
     * @param toUpdateField The field that should be updated within the table
     * @param toUpdateData The data that should be updated within the table
     * @throws SQLException Throw in query not built successfully
     */
    public void update(String table, ArrayList<String> fields, ArrayList<String> data, String toUpdateField, String toUpdateData) throws SQLException {
        Statement statement = conn.createStatement();
        String toExecute = "UPDATE SET " + toUpdateField + " = '" + toUpdateData + "' WHERE ";
    }

    /**
     * Checks if a record exists in a specified table
     * @param table The table to check
     * @param fields The fields to satisfy
     * @param data The data to satisfy
     * @return True if a record was found, false if not
     */
    public boolean recordExists(String table, ArrayList<String> fields, ArrayList<String> data) throws SQLException {
        ResultSet rs = this.select(table, fields, data);
        if (rs.first()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Deletes records from a specified table
     * @param table The table from which the records should be deleted
     * @param fields The fields that should be satisfied
     * @param data The data that should be satisfied
     * @return True if the record was successfully deleted, false if not
     * @throws SQLException If the record wasn't successfully deleted
     */
    public boolean deleteRecord(String table, ArrayList<String> fields, ArrayList<String> data) throws SQLException {
        Statement statement = conn.createStatement();
        String query = "DELETE FROM " + table + " WHERE ";

        for (int i = 0; i < fields.size(); i++) {
            if (i == 0) {
                query += fields.get(i) + " = '" + data.get(i) + "' ";
            } else {
                query += "AND " + fields.get(i) + " = '" + data.get(i) + "' ";
            }
        }

        return statement.execute(query);
    }

    /**
     * Deletes all records from a specified table
     * @return True if successful, false if not
     */
    public boolean deleteAll(String table) throws SQLException {
        Statement statement = conn.createStatement();
        statement.execute("TRUNCATE " + table);
        return true;
    }
}
