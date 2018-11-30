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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
 * @version 1.2
 * @since   1.1
 */
public class DataRecordManagerJunitTest {
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
	private final PrintStream           originalOut = System.out;
	private final PrintStream           originalErr = System.err;
	
	@Before
	public void initialize() throws ClassNotFoundException, SQLException {
		buildConnection();
		setUpStreams();
	}
	
	public void buildConnection() throws ClassNotFoundException, SQLException {
		GenericDbConfig config = new GenericDbConfig("localhost", "3306", "test", "root", null);
		DataRecordManager.buildConnection(DbType.MYSQL, config);
	}
	
	public void setUpStreams() {
		System.setOut(new PrintStream(outContent));
	    System.setErr(new PrintStream(errContent));
	}
	
	@Test
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
		String whereClause = "SampleId = 'A3030301'";
		int originalSizeInDb = DataRecordManager.queryDataRecords("GHSNV", whereClause).size();
		
		DataRecord snv = DataRecordManager.addDataRecord("GHSNV");
		snv.setDataField("SampleId",    "A3030301");
		snv.setDataField("RunId",       "160122_NB501062_00'70_AHWNNNBGYY");
		snv.setDataField("Gene",        "EGFR");
		snv.setDataField("Mutation_AA", "T790M");
		snv.setDataField("Percentage",  9.3);
		snv.setDataField("Chrom",       7);
		snv.setDataField("Position",    1744567441L);
		DataRecordManager.storeAndCommit();
		
		int newSizeInDb = DataRecordManager.queryDataRecords("GHSNV", whereClause).size();
		Assert.assertEquals(originalSizeInDb + 1, newSizeInDb);
	}
	
	@Test
	public void storeAndCommitOnUpdatingExistingRecordTest() throws Exception {
		String whereClause = "SampleId = 'A2049602_1' and Gene = 'BRCA2'";
		List<DataRecord> snvList = DataRecordManager.queryDataRecords("GHSNV", whereClause);
		for (DataRecord snv : snvList) {
			snv.setDataField("Mutation_AA", "P323A");
		}
		DataRecordManager.storeAndCommit();
	}
	
	@Test
	public void queryDataRecordsTest() throws Exception {
		String whereClause = "SampleId = 'A09090101'";
		List<DataRecord> snvList = DataRecordManager.queryDataRecords("GHSNV", whereClause);
		
		Assert.assertEquals(1, snvList.size());
		Assert.assertEquals("A09090101",                       snvList.get(0).getStringVal("SampleId"));
		Assert.assertEquals("160122_NB501062_0070_AHWNNNBGXX", snvList.get(0).getStringVal("RunId"));
		Assert.assertEquals("EGFR",                            snvList.get(0).getStringVal("Gene"));
		Assert.assertEquals("T790M",                           snvList.get(0).getStringVal("Mutation_AA"));
		Assert.assertEquals(new Double(10.5),                  snvList.get(0).getDoubleVal("Percentage"));
		Assert.assertEquals(new Integer(8),                    snvList.get(0).getIntegerVal("Chrom"));
		Assert.assertEquals(new Long(1744567456),              snvList.get(0).getLongVal("Position"));
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
	
		assertThat(diff, IsDataRecordContaining.hasEntry("Gene",        "BRCA2"));
		assertThat(diff, IsDataRecordContaining.hasEntry("Mutation_AA", "R232L"));
		assertThat(diff, IsDataRecordContaining.hasEntry("Percentage",  19.3));
		assertThat(diff, IsDataRecordContaining.hasEntry("Chrom",       10));
		assertThat(diff, IsDataRecordContaining.hasEntry("Position",    1744567441L));
	}
	
	@Test
	public void printDataRecordTest() {
		DataRecord snv = DataRecordManager.addDataRecord("GHSNV");
		snv.setDataField("SampleId",    "A2049602_1");
		snv.setDataField("RunId",       "160122_NB501062_0070_AHWNNNBGXX");
		snv.setDataField("Gene",        "BRCA2");
		snv.setDataField("Mutation_AA", "R232L");
		snv.setDataField("Percentage",  19.3);
		snv.setDataField("Chrom",       10);
		snv.setDataField("Position",    1744567441L);
		
		snv.printDataRecord();
				
		Assert.assertEquals("SampleId A2049602_1\n" + 
				"RunId 160122_NB501062_0070_AHWNNNBGXX\n" + 
				"Gene BRCA2\n" + 
				"Mutation_AA R232L\n" + 
				"Percentage 19.3\n" + 
				"Chrom 10\n" + 
				"Position 1744567441\n", outContent.toString());
	}
	
	@After
	public void cleanUp() throws SQLException {
		closeConnection();
		restoreStreams();
	}
	
	public void closeConnection() throws SQLException {
		DataRecordManager.closeConnection();
	}
	
	public void restoreStreams() {
	    System.setOut(originalOut);
	    System.setErr(originalErr);
	}
}
