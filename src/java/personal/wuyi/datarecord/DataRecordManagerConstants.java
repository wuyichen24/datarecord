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

import java.util.HashMap;
import java.util.Map;

/**
 * Interface to store constants for {@code DataRecordManager}.
 * 
 * @author  Wuyi Chen
 * @date    03/07/2016
 * @version 1.1
 * @since   1.1
 */
interface DataRecordManagerConstants {
	/**
	 * The primary key (column name) for identifying two different records in 
	 * database.
	 */
	String RECORD_IDENTIFIER = "RecordId";
	
	/**
	 * Mapping between the types of MySQL column and Java primitive type:
	 * <ul>
	 * 	<li>VARCHAR => String
	 * 	<li>INT => Integer
	 * 	<li>BIGINT => Long
	 * 	<li>DOUBLE => Double
	 * </ul>
	 */
	Map<String, Class<?>> COLUMN_TYPE_MAP = new HashMap<String, Class<?>>() {
		private static final long serialVersionUID = 1L;
		
		{
			put("VARCHAR", String.class);
			put("INT",     Integer.class);
			put("BIGINT",  Long.class);
			put("DOUBLE",  Double.class);
		}
	};
	
	/** The template for select statements. */
	String SELECT_STATEMENT = "SELECT * FROM ";
}
