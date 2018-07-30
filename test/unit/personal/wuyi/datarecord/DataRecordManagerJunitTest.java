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

import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.hamcrest.collection.IsMapContaining;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import personal.wuyi.datarecord.DataRecordManager;
import personal.wuyi.client.database.DbType;
import personal.wuyi.client.database.GenericDbConfig;

/**
 * Test class for {@code DataRecordManager}.
 * 
 * @author  Wuyi Chen
 * @date    07/12/2018
 * @version 1.1
 * @since   1.1
 */
public class DataRecordManagerJunitTest {	
	@Before
	public void initialize() throws ClassNotFoundException, SQLException {
		GenericDbConfig config = new GenericDbConfig("localhost", "3306", "test", "root", null);
		DataRecordManager.buildConnection(DbType.MYSQL, config);
	}
	
	public void addDataRecord() {
		int originalSizeOfCommitPool = DataRecordManager.getSizeOfCommitPool();
		DataRecordManager.addDataRecord("GHSNV");	
		int newSizeOfCommitPool = DataRecordManager.getSizeOfCommitPool();
		Assert.assertEquals(originalSizeOfCommitPool + 1, newSizeOfCommitPool);
	}
	
	@Test
	public void getColumnMetadataTest() throws Exception {
		Map<String, Class<?>> columnTypes = DataRecordManager.getColumnMetadata("GHSNV");
		
		assertThat(columnTypes, IsMapContaining.hasEntry("RecordId",    Long.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("SampleId",    String.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("RunId",       String.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("Gene",        String.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("Mutation_AA", String.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("Percentage",  Double.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("Chrom",       Integer.class));
		assertThat(columnTypes, IsMapContaining.hasEntry("Position",    Long.class));
	}
	
	@Test 
	public void storeAndCommitOnInsertingNewRecordTest() throws Exception {
		DataRecord snv = DataRecordManager.addDataRecord("GHSNV");
		snv.setDataField("SampleId",    "A2049602_1");
		snv.setDataField("RunId",       "160122_NB501062_00'70_AHWNNNBGYY");
		snv.setDataField("Gene",        "EGFR");
		snv.setDataField("Mutation_AA", "T790M");
		snv.setDataField("Percentage",  9.3);
		snv.setDataField("Chrom",       7);
		snv.setDataField("Position",    1744567441L);
		DataRecordManager.storeAndCommit();
	}
	
	
	public void storeAndCommitOnUpdatingExistingRecordTest() throws Exception {
		String whereClause = "SampleId = 'A2049602_1' and Gene = 'BRCA2'";
		List<DataRecord> snvList = DataRecordManager.queryDataRecords("GHSNV", whereClause);
		for (DataRecord snv : snvList) {
			snv.setDataField("Mutation_AA", "P323A");
		}
		DataRecordManager.storeAndCommit();
	}
	
	
	public void queryDataRecordsTest() throws Exception {
		String whereClause = "SampleId = 'A2049602_1'";
		List<DataRecord> snvList = DataRecordManager.queryDataRecords("GHSNV", whereClause);
		for (DataRecord snv : snvList) {
			snv.printDataRecord();
			System.out.println("========================");
		}	
	}

	@Test
	public void compareAndGetDiffTest() {
		DataRecord snv1 = DataRecordManager.addDataRecord("GHSNV");
		snv1.setDataField("SampleId",    "A2049602_1");
		snv1.setDataField("RunId",       "160122_NB501062_0070_AHWNNNBGXX");
		snv1.setDataField("Gene",        "EGFR");
		snv1.setDataField("Mutation_AA", "T790M");
		snv1.setDataField("Percentage",  9.3);
		snv1.setDataField("Chrom",       7);
		snv1.setDataField("Position",    1744567441L);
		
		DataRecord snv2 = DataRecordManager.addDataRecord("GHSNV");
		snv2.setDataField("SampleId",    "A2049602_1");
		snv2.setDataField("RunId",       "160122_NB501062_0070_AHWNNNBGXX");
		snv2.setDataField("Gene",        "BRCA2");
		snv2.setDataField("Mutation_AA", "R232L");
		snv2.setDataField("Percentage",  19.3);
		snv2.setDataField("Chrom",       10);
		snv2.setDataField("Position",    1744567441L);
		
		DataRecord diff = DataRecordManager.compareAndGetDiff(snv1, snv2);
	
		diff.printDataRecord();
	}
	
	@After
	public void closeConnection() throws SQLException {
		DataRecordManager.closeConnection();
	}
}
