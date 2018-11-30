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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Generic data type for dynamic number of fields and different type of fields.
 * 
 * <p>The structure of {@code DataRecord} is similar to the {@code Map}. But 
 * the main difference is it allows values to have different type. And 
 * {@code DataRecordManager} also provides convenient and transaction-based 
 * APIs for synchronizing between the {@code DataRecord} and the database. 
 * 
 * <p>Object serialization is the process of saving an object's state to a 
 * sequence of bytes. Normal objects exist only as long as the Java virtual 
 * machine remains running. With object serialization, the objects we create 
 * could exist beyond the lifetime of the virtual machine.
 * 
 * @author  Wuyi Chen
 * @date    03/07/2016
 * @version 1.1
 * @since   1.1
 */
public class DataRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * The type of DataRecord
	 * 
	 * <p>The type of DataRecord should be totally matched with the table name
	 * in the database.
	 */
	private String dataType;
	
	/**
	 * The hash map to store the value of each field.
	 * 
	 * <p>All the values will be converted to Object to store into this 
	 * collection.
	 * 
	 * <p>The key of the pairs in this value hash map should be totally 
	 * matched with the type hash map.
	 */
	private transient Map<String, Object> valueMap;
	
	/**
	 * The hash map to store the type of each field.
	 */
	private transient Map<String, Class<?>> typeMap;
	
	/**
	 * The flag indicates this {@code DataRecord} has been modified in memory 
	 * or not.
	 */
	private boolean isModified;
	
	/**
	 * The flag indicates database already had the corresponding record for 
	 * this {@code DataRecord} or not.
	 */
	private boolean isNewRecordForDatabase;
	
	/**
	 * The identifier to specify two different {@code DataRecord}.
	 */
	private long recordId;
	
	/**
	 * Construct a {@code DataRecord}.
	 * 
	 * @param  dataType
	 *         The table name in the database matches to this 
	 *         {@code DataRecord}.
	 *         
	 * @since   1.1
	 */
	public DataRecord(final String dataType) {
		Preconditions.checkNotNull(dataType);
		
		this.dataType          = dataType;
		valueMap               = new LinkedHashMap<>();
		typeMap                = new LinkedHashMap<>();
		isModified             = true;
		isNewRecordForDatabase = true;
	}
	
	/**
	 * Construct a {@code DataRecord}.
	 * 
	 * @param  dataType
	 *         The table name in the database matches to this 
	 *         {@code DataRecord}.
	 *         
	 * @param  isNewRecordForDatabase
	 *         The flag to indicate this {@code DataRecord} is new to the 
	 *         database.
	 *         
	 * @since   1.1
	 */
	public DataRecord(final String dataType, final boolean isNewRecordForDatabase) {
		Preconditions.checkNotNull(dataType);
		
		this.dataType               = dataType;
		valueMap                    = new LinkedHashMap<>();
		typeMap                     = new LinkedHashMap<>();
		isModified                  = true;
		this.isNewRecordForDatabase = isNewRecordForDatabase;
	}
	
	protected boolean isModified()                                                  { return isModified;                                    }
	protected void    setModified(final boolean isModified)                         { this.isModified = isModified;                         }
	protected boolean isNewRecordForDatabase()                                      { return isNewRecordForDatabase;                        }
	protected void    setNewRecordForDatabase(final boolean isNewRecordForDatabase) { this.isNewRecordForDatabase = isNewRecordForDatabase; }
	protected long    getRecordId()                                                 { return recordId;                                      }
	protected void    setRecordId(final long recordId)                              { this.recordId = recordId;                             }

	/**
	 * Set a new field as {@code String} value.
	 * 
	 * <p>If the {@code DataRecord} previously contained a mapping for the 
	 * field, the old value is replaced by the new value.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @param  value
	 *         The string value needs to be set for the field.
	 *         
	 * @since   1.1
	 */
	public void setDataField(final String fieldName, final String value) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		valueMap.put(fieldName, value);
		typeMap.put(fieldName, String.class);
		setModified(true);
	}
	
	/**
	 * Set a new field as {@code Integer} value.
	 * 
	 * <p>If the {@code DataRecord} previously contained a mapping for the 
	 * field, the old value is replaced by the new value.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @param  value
	 *         The integer value needs to be set for the field.
	 *         
	 * @since   1.1
	 */
	public void setDataField(final String fieldName, final Integer value) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		valueMap.put(fieldName, value);
		typeMap.put(fieldName, Integer.class);
		setModified(true);
	}
	
	/**
	 * Set a new field as {@code Long} value.
	 * 
	 * <p>If the {@code DataRecord} previously contained a mapping for the 
	 * field, the old value is replaced by the new value.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 * 
	 * @param  value
	 *         The long value needs to be set for the field.
	 *         
	 * @since   1.1
	 */
	public void setDataField(final String fieldName, final Long value) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		valueMap.put(fieldName, value);
		typeMap.put(fieldName, Long.class);
		setModified(true);
	}
	
	/**
	 * Set a new field as {@code Double} value.
	 * 
	 * <p>If the {@code DataRecord} previously contained a mapping for the 
	 * field, the old value is replaced by the new value.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 * 
	 * @param  value
	 *         The long value needs to be set for the field.
	 *         
	 * @since   1.1
	 */
	public void setDataField(final String fieldName, final Double value) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		valueMap.put(fieldName, value);
		typeMap.put(fieldName, Double.class);
		setModified(true);
	}
	
	/**
	 * Get the {@code String} value of a field.
	 * 
	 * <p>This function will check the field's existence and check the type of 
	 * field as {@code String}.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 * 
	 * @return  The string value mapped to the field.
	 * 
	 * @since   1.1
	 */
	public String getStringVal(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		
		checkField(fieldName, String.class);
		return (String) valueMap.get(fieldName);
	}
	
	/**
	 * Get the {@code Integer} value of a field.
	 * 
	 * <p>This function will check the field's existence and check the type of 
	 * field as {@code Integer}.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @return  The integer value mapped to the field.
	 * 
	 * @since   1.1
	 */
	public Integer getIntegerVal(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		
		checkField(fieldName, Integer.class);
		return (Integer) valueMap.get(fieldName);
	}
	
	/**
	 * Get the {@code Long} value of a field.
	 * 
	 * <p>This function will check the field's existence and check the type of 
	 * field as {@code Long}.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 * 
	 * @return  The long value mapped to the field.
	 * 
	 * @since   1.1
	 */
	public Long getLongVal(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		
		checkField(fieldName, Long.class);
		return (Long) valueMap.get(fieldName);
	}
	
	/**
	 * Get the {@code Double} value of a field.
	 * 
	 * <p>This function will check the field's existence and check the type of 
	 * field as {@code Double}.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 * 
	 * @return  The double value mapped to the field.
	 * 
	 * @since   1.1
	 */
	public Double getDoubleVal(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		
		checkField(fieldName, Double.class);
		return (Double) valueMap.get(fieldName);
	}
	
	/**
	 * Get the {@code Map} which stores the value of each field.
	 * 
	 * @return  The map contains the values of all the field.
	 * 
	 * @since   1.1
	 */
	public Map<String, Object> getValueFields() {
		return valueMap;
	}
	
	/**
	 * Get the {@code Map} which stores the type of each field.
	 * 
	 * @return  The map contains the types of all the field.
	 * 
	 * @since   1.1
	 */
	public Map<String, Class<?>> getTypeFields() {
		return typeMap;
	}
	
	/**
	 * Get the type/table name of the {@code DataRecord}.
	 * 
	 * @return  The type of the {@code DataRecord}.
	 * 
	 * @since   1.1
	 */
	public String getDataTypeName() {
		return dataType;
	}
	
	/**
	 * Get the value of a field in {@code Object} type.
	 * 
	 * <p>This function will check the field's existence and return the value 
	 * as {@code Object}. 
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @return  The value mapped to the field in {@code Object}.
	 * 
	 * @since   1.1
	 */
	public Object getValue(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		
		checkFieldExist(fieldName);
		return valueMap.get(fieldName);
	}
	
	/**
	 * Check the field's existence and the type of the field.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @param  expectedFieldType
	 *         The expected type of the field.
	 * @return  {@code true} if the field passed this check;
	 *          {@code false} otherwise.
	 *          
	 * @since   1.1
	 */
	protected boolean checkField(final String fieldName, final Class<?> expectedFieldType) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(expectedFieldType);
		
		checkFieldExist(fieldName);
		checkFieldType(fieldName, expectedFieldType);
		return true;
	}
	
	/**
	 * Check the field's existence in the {@code DataRecord}.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @return  {@code true} if the field is existing in the {@code DataRecord};
	 *          {@code false} otherwise. 
	 *          
	 * @since   1.1
	 */
	protected boolean checkFieldExist(final String fieldName) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		if (!valueMap.containsKey(fieldName) || !typeMap.containsKey(fieldName)) {
			throw new NoSuchElementException("Not existing field: " + fieldName);
		}
		return true;
	}
	
	/**
	 * Check the type of the field.
	 * 
	 * @param  fieldName
	 *         The name of the field.
	 *         
	 * @param  expectedFieldType
	 *         The expected type for the field.
	 * 
	 * @return  {@code true} if the type of the field is as expected.
	 *          {@code false} otherwise.
	 *          
	 * @since   1.1
	 */
	protected boolean checkFieldType(final String fieldName, final Class<?> expectedFieldType) {
		Preconditions.checkNotNull(fieldName);
		Preconditions.checkNotNull(expectedFieldType);
		Preconditions.checkNotNull(typeMap);
		
		if (typeMap.get(fieldName) != expectedFieldType){
			throw new IllegalArgumentException(fieldName + " is not " + expectedFieldType);
		}
		return true;
	}

	
	/**
	 * Print all values of a {@code DataRecord}.
	 * 
	 * @since   1.1
	 */
	public void printDataRecord() {
		Preconditions.checkNotNull(valueMap);
		Preconditions.checkNotNull(typeMap);
		
		for (Map.Entry<String, Object> entry : valueMap.entrySet()) {			
			if (typeMap.get(entry.getKey()) == String.class) {
				System.out.println(entry.getKey() + " " + (String) entry.getValue());
			}
			if (typeMap.get(entry.getKey()) == Integer.class) {
				System.out.println(entry.getKey() + " " + (Integer)entry.getValue());
			}
			if (typeMap.get(entry.getKey()) == Long.class) {
				System.out.println(entry.getKey() + " " + (Long) entry.getValue());
			}
			if (typeMap.get(entry.getKey()) == Double.class) {
				System.out.println(entry.getKey() + " " + (Double) entry.getValue());
			}
		}
	}
}
