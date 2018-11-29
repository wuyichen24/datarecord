/*
 * Copyright 2018 Wuyi Chen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package personal.wuyi.datarecord;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import personal.wuyi.client.database.DbType;
import personal.wuyi.client.database.GenericDbConfig;
import personal.wuyi.string.SqlUtil;

import com.google.common.base.Preconditions;

/**
 * Static class for handing basic operations between the {@code DataRecord} 
 * and the database.
 * 
 * This static class provides convenient and transaction-based APIs for 
 * synchronizing between the {@code DataRecord} and the database.
 * 
 * @author  Wuyi Chen
 * @date    03/07/2016
 * @version 1.1
 * @since   1.1
 */
public class DataRecordManager implements DataRecordManagerConstants {
	/** The type of the database */
	protected static DbType     type;
	protected static Connection connect;
	
	/**
	 * <p>This commit pool is to store all DataRecords in memory temporarily.
	 * The DataRecord in this pool is waiting to be synchronized to database 
	 * or modified in memory.
	 * 
	 * <p>Run addDataRecord() function will create and add a new DataRecord 
	 * into this commit pool.
	 * 
	 * <p>Run storeAndCommit() function will synchronize all the DataRecords 
	 * in this commit pool with database and clean up this commit pool.
	 */
	protected static List<DataRecord> commitPool = new ArrayList<>();
	
	private DataRecordManager() {}
	
	/**
	 * Build database connection.
	 * 
	 * @param  type
	 *         The database type.
	 *         
	 * @param  config
	 *         The generic database configuration.
	 * 
	 * @throws  SQLException
	 *          If there is any error when connecting to database.
	 *          
	 * @throws  ClassNotFoundException
	 *          If the driver class is not found in the class path.
	 *          
	 * @since   1.1
	 */
	public static void buildConnection(final DbType type, final GenericDbConfig config) throws SQLException, ClassNotFoundException {
		commitPool = new ArrayList<>();
		DataRecordManager.type = type;
		Class.forName(type.getDriverClass());
		if (connect == null || connect.isClosed()) {
			connect = DriverManager.getConnection(type.buildUrl(config), config.getUsername(), config.getPassword());
		}
	}
	
	/**
	 * Close database connection.
	 * 
	 * <p>MySQL has a parameter called "wait_timeout". The default value is 8 
	 * hours. If there is no activity in 8 hours, MySQL would disable that 
	 * connection. So in that case, you must close the previous connection and 
	 * re-build a new connection.
	 *
	 * @throws  SQLException
	 *          If there is any error when closing the connection.
	 *          
	 * @since   1.1
	 */
	public static void closeConnection() throws SQLException {
		if (connect != null && !connect.isClosed()) {
			connect.close();
		}
	}
	
	/**
	 * Create a new {@code DataRecord} object.
	 * 
	 * <p>This function will create and return a blank {@code DataRecord} 
	 * object for developers to message a new record. Also, this function will 
	 * add this new {@code DataRecord} object to the internal commit pool of 
	 * {@code DataRecordManager}.
	 * 
	 * @param  dataType
	 *         The table name in the database matches to the new 
	 *         {@code DataRecord}.
	 *         
	 * @return  The new created {@code DataRecord}.
	 * 
	 * @since   1.1
	 */
	public static DataRecord addDataRecord(final String dataType) {
		Preconditions.checkNotNull(commitPool);
		
		final DataRecord newDataRecord = new DataRecord(dataType);
		commitPool.add(newDataRecord);
		return newDataRecord;
	}
	
	/**
	 * Synchronize all the {@code DataRecord}s in this commit pool with 
	 * database.
	 * 
	 * <p>First, this method will verify the type of each field are matching 
	 * the type of the corresponding column in the database. 
	 * 
	 * <p>Second, this method will try to synchronize all the 
	 * {@code DataRecord}s in commit pool with database.
	 * 
	 * @throws  SQLException
	 *          If the type of a field in the {@code DataRecord} can not match 
	 *          the type of the column in the database, or there is an error 
	 *          occurred when committing the result.
	 *          
	 * @since   1.1
	 */
	public static void storeAndCommit() throws SQLException {
		Preconditions.checkNotNull(commitPool);
		
		for (DataRecord dataRecord : commitPool) {
			verifyDataField(dataRecord.getDataTypeName(), dataRecord.getTypeFields());
		}
		
		for(DataRecord dataRecord : commitPool) {
			synchronizeDataRecordWithDatabase(dataRecord);
		}
	}
	
	/**
	 * Verify the type of each field in the {@code DataRecord} can match the 
	 * type of the columns in the database.
	 * 
	 * @param  dataRecord
	 *         The {@code DataRecord} needs to be checked.
	 *         
	 * @throws  SQLException
	 *          If the type of a field in the {@code DataRecord} can not match 
	 *          the type of the column in the database.
	 *          
	 * @since   1.1
	 */
	protected static void verifyDataField(final DataRecord dataRecord) throws SQLException {
		Preconditions.checkNotNull(dataRecord);
		verifyDataField(dataRecord.getDataTypeName(), dataRecord.getTypeFields());
	}
	
	/**
	 * Verify the type of each field in the {@code DataRecord} can match the 
	 * type of the columns in the database.
	 * 
	 * <p>First, this method will get the meta data of the table based on the 
	 * data type of the {@code DataRecord}.
	 * 
	 * <p>Second, this method will get all the data fields in the 
	 * {@code DataRecord} and verify the column type matching between the 
	 * {@code DataRecord} and the database table.
	 * 
	 * @param  tableName
	 *         The table name.
	 * 
	 * @param  typeMap
	 *         The map contains the field names and the field types.
	 * 
	 * @throws  SQLException
	 *          If the type of a field in the {@code DataRecord} can not match 
	 *          the type of the column in the database.
	 *          
	 * @since   1.1
	 */
	protected static void verifyDataField(final String tableName, final Map<String, Class<?>> typeMap) throws SQLException{
		Preconditions.checkNotNull(tableName);
		Preconditions.checkNotNull(typeMap);
		
		final Map<String, Class<?>> columnTypes = getColumnMetadata(tableName);    // Get relevant table schema from database
		
		for (Map.Entry<String, Class<?>> entry : typeMap.entrySet()) {
			if (columnTypes.containsKey(entry.getKey())) {
				if (entry.getValue() == columnTypes.get(entry.getKey())) {
					continue;
				} else {
					throw new SQLException("The type of " + entry.getKey() + " is " + entry.getValue() 
							+ ". Can not match " + columnTypes.get(entry.getKey()) + " in table " + tableName);
				}
			} else {
				throw new SQLException(tableName + " doesn't has column: " + entry.getKey());
			}
		}
	}
	
	/**
	 * Get the metadata of a table in database.
	 * 
	 * <p>This function will get the metadata of a table and then
	 * convert the type of column to Java primitive type and message
	 * as a {@code Map}.
	 * 
	 * <p>The relationship between the MySQL column types and Java 
	 * primitive types:
	 * <ul>
	 * 	<li>VARCHAR => String
	 * 	<li>INT => Integer
	 * 	<li>BIGINT => Long
	 * 	<li>DOUBLE => Double
	 * </ul>
	 * 
	 * @param  tableName
	 *         The name of the table needs to be inquired.
	 *         
	 * @return  The map of the field name and the type of the field.
	 * 
	 * @throws  SQLException
	 *          If the error occurred when querying the database.
	 *          
	 * @since   1.1
	 */
	protected static Map<String, Class<?>> getColumnMetadata(final String tableName) throws SQLException {
		Preconditions.checkNotNull(connect);
		Preconditions.checkArgument(!isNullOrEmpty(tableName), "tableName is null or empty");
		
		final Map<String, Class<?>> columnTypes = new LinkedHashMap<>();
		
		try (final Statement statement   = connect.createStatement();
		     final ResultSet rs          = statement.executeQuery(SELECT_STATEMENT + tableName)) {
			final ResultSetMetaData     metadata    = rs.getMetaData();
			for (int i = 1; i <= metadata.getColumnCount(); i++) {
				columnTypes.put(metadata.getColumnName(i), COLUMN_TYPE_MAP.get(metadata.getColumnTypeName(i)));
			}
		}
			
		return columnTypes; 
	}
	
	/**
	 * Synchronize one {@code DataRecord} with database.
	 * 
	 * <p>If database already had a corresponding record for that 
	 * {@code DataRecord}, just need to use the update operation. If the 
	 * {@code DataRecord} is a new one for database, this method needs to call 
	 * insert operation.
	 * 
	 * @param  dataRecord
	 *         The {@code DataRecord} needs to be synchronized.
	 *         
	 * @throws  SQLException
	 *          If the error occurred when updating a existing record or 
	 *          inserting a new record with database.
	 *          
	 * @since   1.1
	 */
	protected static void synchronizeDataRecordWithDatabase(final DataRecord dataRecord) throws SQLException {
		Preconditions.checkNotNull(dataRecord);
		
		if(dataRecord.isNewRecordForDatabase()) {
			insertDataRecordBase(dataRecord);
		} else {
			updateDataRecord(dataRecord);
		}
	}

	/**
	 * Update the {@code DataRecord} in memory to the database.
	 * 
	 * <p>The function will use RecordId to map the record in database and 
	 * the corresponding {@code DataRecord} in memory. And then compare the 
	 * difference between them and update the record in database.
	 * 
	 * <p>The process of this function:
	 * <ul>
	 * 	<li>Use RecordId to query the database and get record as DataRecord.
	 *  <li>Check there is only one record for a certain RecordId in database.
	 *  <li>Compare two DataRecords between database one and memory one. 
	 *      Construct a new DataRecord which captures the difference.
	 *  <li>Update the corresponding record in database.
	 * </ul>
	 * 
	 * @param  dataRecordMem
	 *         The {@code DataRecord} in memory.
	 *         
	 * @throws SQLException
	 *         If the error occurred when querying the database or updating 
	 *         the database.
	 *         
	 * @since   1.1
	 */
	protected static void updateDataRecord(final DataRecord dataRecordMem) throws SQLException {
		Preconditions.checkNotNull(dataRecordMem);
		
		final String whereClause = RECORD_IDENTIFIER + " = " + dataRecordMem.getRecordId();
		final List<DataRecord> dataRecordList = queryDataRecordsBase(dataRecordMem.getDataTypeName(), whereClause);
		
		if (dataRecordList.size() != 1) {
			if (dataRecordList.isEmpty()) {
				throw new SQLException(dataRecordMem.getDataTypeName() + " doesn't have a record for " + RECORD_IDENTIFIER + ": " + dataRecordMem.getRecordId());
			} else if (dataRecordList.size() > 1) {
				throw new SQLException(dataRecordMem.getDataTypeName() + " has multiple records for " + RECORD_IDENTIFIER + ": " + dataRecordMem.getRecordId());
			}
		}
		final DataRecord dataRecordDb = dataRecordList.get(0);
		
		final DataRecord diff = compareAndGetDiff(dataRecordDb, dataRecordMem);
		diff.setRecordId(dataRecordDb.getRecordId());
		
		updateDataRecordBase(diff);
	}

	/**
	 * Query a list of {@code DataRecord}s from database.
	 * 
	 * <p>After querying the list of {@code DataRecord}s. The list will be also added 
	 * to commitPool for updating back to the database later on.
	 * 
	 * @param  dataType
	 *         The name of table needs to be queried.
	 *         
	 * @param  whereClause
	 *         The where clause of the query.
	 *         
	 * @return  The list of {@code DataRecord}s.
	 * 
	 * @throws  SQLException 
	 *          If an error occurred when querying the database.
	 *          
	 * @since   1.1
	 */
	public static List<DataRecord> queryDataRecords(final String dataType, final String whereClause) throws SQLException  {
		Preconditions.checkArgument(!isNullOrEmpty(dataType), "dataType is null or empty");
		
		final List<DataRecord> dataRecordList = queryDataRecordsBase(dataType, whereClause);
		commitPool.addAll(dataRecordList);
		return dataRecordList;
	}
	
	/**
	 * Insert one {@code DataRecord} into database.
	 * 
	 * @param  dataRecord
	 *         The {@code DataRecord} needs to be inserted.
	 * 
	 * @throws  SQLException
	 *          If an error occurred when inserting the record into database.
	 *          
	 * @since   1.1
	 */
	protected static void insertDataRecordBase(final DataRecord dataRecord) throws SQLException {
		Preconditions.checkNotNull(connect);
		
		try (final Statement statement = connect.createStatement()) {
			final String sqlStatement = generateSQLInsertStatement(dataRecord);
			System.out.println(sqlStatement);
			statement.executeUpdate(sqlStatement);	
		}
	}
	
	/**
	 * Update one {@code DataRecord} into database.
	 * 
	 * @param  dataRecord
	 *         The {@code DataRecord} needs to be updated.
	 *         
	 * @throws  SQLException
	 *          If an error occurred when updating the record into database.
	 *          
	 * @since   1.1
	 */
	protected static void updateDataRecordBase(final DataRecord diff) throws SQLException {
		try (final Statement statement = connect.createStatement()) {
			final String sqlStatement = generateSQLUpdateStatement(diff);
			statement.executeUpdate(sqlStatement);
		}
	}
	
	/**
	 * Query a list of {@code DataRecord}s from database.
	 * 
	 * <p>This method will query the table and get a list of matched results. 
	 * And message the results to the list of {@code DataRecord}s.
	 * 
	 * @param  dataType
	 *         The name of the table.
	 * 
	 * @param  whereClause
	 *         The where clause of the query.
	 *         
	 * @return  The list of {@code DataRecord}s.
	 * 
	 * @throws  SQLException
	 *          If an error occurred when querying the database.
	 *          
	 * @since   1.1
	 */
	protected static List<DataRecord> queryDataRecordsBase(final String dataType, final String whereClause) throws SQLException {
		Preconditions.checkNotNull(connect);
		Preconditions.checkNotNull(commitPool);
		
		final List<DataRecord> dataRecordList = new ArrayList<>();
		final String sqlStatement = generateSQLQueryStatement(dataType, whereClause);
		
		try (final Statement statement = connect.createStatement();
			 final ResultSet rs        = statement.executeQuery(sqlStatement)) {
		
			final Map<String, Class<?>> columnTypes = getColumnMetadata(dataType);
		
			while(rs.next()) {
				final DataRecord dataRecord = new DataRecord(dataType, false);            // specify this DataRecord is not new one to database
				dataRecord.setRecordId(rs.getLong(RECORD_IDENTIFIER));
				for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
					dataRecord.getTypeFields().put(entry.getKey(), entry.getValue());
					if (entry.getValue() == String.class) {
						dataRecord.getValueFields().put(entry.getKey(), rs.getString(entry.getKey()));
					}
					if (entry.getValue() == Integer.class) {
						dataRecord.getValueFields().put(entry.getKey(), rs.getInt(entry.getKey()));
					}
					if (entry.getValue() == Long.class) {
						dataRecord.getValueFields().put(entry.getKey(), rs.getLong(entry.getKey()));
					}
					if (entry.getValue() == Double.class) {
						dataRecord.getValueFields().put(entry.getKey(), rs.getDouble(entry.getKey()));
					}
				}
				dataRecordList.add(dataRecord);
			}
		}
		
		return dataRecordList;
	}
	
	/**
	 * Generate the SQL insert statement for one {@code DataRecord}.
	 * 
	 * <p>This function is to generate SQL insert statement and return the SQL
	 * statement like: 
	 * <pre>
	 * INSERT INTO table (column1, column2, column3) 
	 * VALUES (abc, 123, 12.4)
	 * </pre>
	 * 
	 * <p>This function will automatically handle the mapping between each 
	 * column name and its values. It totally avoids developer to write a long 
	 * SQL statement or waste time on checking column mapping. 
	 * 
	 * <p>Also, developers also don't need to leave a white space when they 
	 * are trying to message SQL statement.
	 * 
	 * @param  dataRecord
	 *         The {@code DataRecord} needs to be inserted.
	 *         
	 * @return  The SQL insert statement.
	 * 
	 * @since   1.1
	 */
	protected static String generateSQLInsertStatement(final DataRecord dataRecord) {
		final StringBuilder sqlStatement = new StringBuilder();
		final StringBuilder columnSb     = new StringBuilder();
		final StringBuilder valueSb      = new StringBuilder();
		
		for (Map.Entry<String, Object> entry : dataRecord.getValueFields().entrySet()) {
			// add column name
			columnSb.append(entry.getKey()).append(",");
			
			// add value
			if (dataRecord.getTypeFields().get(entry.getKey()) == String.class) {
				valueSb.append("'").append(SqlUtil.insertValueWithSingleQuote((String) entry.getValue())).append("'").append(",");  // double the single quote if the value is string
			}
			if (dataRecord.getTypeFields().get(entry.getKey()) == Integer.class) {
				valueSb.append((Integer)entry.getValue()).append(",");
			}
			if (dataRecord.getTypeFields().get(entry.getKey()) == Long.class) {
				valueSb.append((Long) entry.getValue()).append(",");
			}
			if (dataRecord.getTypeFields().get(entry.getKey()) == Double.class) {
				valueSb.append((Double) entry.getValue()).append(",");
			}
		}
		
		// Build SQL statement
		sqlStatement.append("INSERT INTO " + dataRecord.getDataTypeName() + " ");
		sqlStatement.append("(").append(columnSb.substring(0, columnSb.length()-1)).append(") ");
		sqlStatement.append("VALUES ");
		sqlStatement.append("(").append(valueSb.substring(0, valueSb.length()-1)).append(")");
		
		return sqlStatement.toString();
	}
	
	/**
	 * Generate SQL update statement for one {@code DataRecord}.
	 * 
	 * <p>This function is to generate SQL update statement and return the SQL
	 * statement like: 
	 * <pre>
	 * UPDATE table SET column1='abc', column2=12 
	 * WHERE RecordId = 123.
	 * </pre>
	 * 
	 * <p>This function will automatically handle the mapping between each 
	 * column name and it updated value. It totally avoids developer to write 
	 * a long SQL statement or waste time on checking column mapping.
	 * 
	 * <p>Also, developers also don't need to leave a white space when they 
	 * are trying to message SQL statement.
	 * 
	 * @param  diff
	 *         The {@code DataRecord} to capture the fields needs to be updated.
	 *  
	 * @return  The SQL update statement.
	 * 
	 * @since   1.1
	 */
	protected static String generateSQLUpdateStatement(final DataRecord diff) {
		final StringBuilder sqlStatement = new StringBuilder();
		final StringBuilder assignSb     = new StringBuilder();
		
		for (Map.Entry<String, Object> entry : diff.getValueFields().entrySet()) {
			if (diff.getTypeFields().get(entry.getKey()) == String.class) {
				assignSb.append(entry.getKey()).append("=").append("'").append(SqlUtil.insertValueWithSingleQuote((String) entry.getValue())).append("'").append(",");  // double the single quote if the value is string
			}
			if (diff.getTypeFields().get(entry.getKey()) == Integer.class) {
				assignSb.append(entry.getKey()).append("=").append((Integer)entry.getValue()).append(",");
			}
			if (diff.getTypeFields().get(entry.getKey()) == Long.class) {
				assignSb.append(entry.getKey()).append("=").append((Long) entry.getValue()).append(",");
			}
			if (diff.getTypeFields().get(entry.getKey()) == Double.class) {
				assignSb.append(entry.getKey()).append("=").append((Double) entry.getValue()).append(",");
			}
		}
		
		// Build SQL statement
		sqlStatement.append("UPDATE " + diff.getDataTypeName() + " ");
		sqlStatement.append("SET ").append(assignSb.substring(0, assignSb.length()-1)).append(" ");
		sqlStatement.append("WHERE " + RECORD_IDENTIFIER + " = " + diff.getRecordId());
		
		return sqlStatement.toString();
	}
	
	/**
	 * Generate SQL query statement for one {@code DataRecord}.
	 * 
	 * <p>This function is to generate SQL query statement and return the SQL
	 * statement like: 
	 * <pre>
	 * SELECT * FROM table WHERE column = 'abc'.
	 * </pre>
	 * 
	 * <p>This function will automatically handle the mapping between each 
	 * column name and its values.
	 * 
	 * @param  dataType
	 *         The name of the table.
	 *         
	 * @param  whereClause
	 *         The where clause.
	 *         
	 * @return  The SQL query statement.
	 * 
	 * @since   1.1
	 */
	protected static String generateSQLQueryStatement(final String dataType, final String whereClause) {
		Preconditions.checkArgument(!isNullOrEmpty(dataType), "dataType is null or empty");
		
		final StringBuilder sqlStatement = new StringBuilder();
		if (isNullOrEmpty(whereClause)) {
			sqlStatement.append(SELECT_STATEMENT + dataType);
		} else {
			sqlStatement.append(SELECT_STATEMENT + dataType + " WHERE ").append(whereClause);
		}
		return sqlStatement.toString();
	}
	
	/**
	 * Compare two {@code DataRecord}s.
	 * 
	 * <p>This method will compare two {@code DataRecord}s and return a new 
	 * {@code DataRecord} to capture the value difference between them by 
	 * populating the new {@code DataRecord} with the values in df2.
	 * 
	 * @param  dr1
	 *         The first {@code DataRecord}.
	 *         
	 * @param  dr2
	 *         The second {@code DataRecord}.
	 *         
	 * @return  The new {@code DataRecord} to reflect the differences by the 
	 *          field name and the values in the dr2.
	 *          
	 * @since   1.1
	 */
	protected static DataRecord compareAndGetDiff(final DataRecord dr1, final DataRecord dr2) {
		Preconditions.checkNotNull(dr1);
		Preconditions.checkNotNull(dr2);
		Preconditions.checkArgument(dr1.getDataTypeName().equalsIgnoreCase(dr2.getDataTypeName()));
		
		final DataRecord diff = new DataRecord(dr1.getDataTypeName());
		
		for (Map.Entry<String, Object> entry : dr1.getValueFields().entrySet()) {
			if(entry.getValue() != dr2.getValueFields().get(entry.getKey())) {
				diff.getValueFields().put(entry.getKey(), dr2.getValueFields().get(entry.getKey()));         // the new value should be based on dr2
				diff.getTypeFields().put(entry.getKey(), dr2.getTypeFields().get(entry.getKey()));
			}
		}
		
		return diff;
	}
	
	/**
	 * Get the size of the commit pool in the {@code DataRecordManager}.
	 * 
	 * @return  The size of the commit pool.
	 * 
	 * @since   1.1
	 */
	public static int getSizeOfCommitPool() {
		return commitPool.size();
	}
}
