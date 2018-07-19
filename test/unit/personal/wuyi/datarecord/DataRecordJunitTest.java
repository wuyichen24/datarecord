package personal.wuyi.datarecord;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.hamcrest.collection.IsMapContaining;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test class for {@code DataRecord}.
 * 
 * @author  Wuyi Chen
 * @date    07/12/2018
 * @version 1.1
 * @since   1.1
 */
public class DataRecordJunitTest {
	public DataRecord setDataRecord() {
		DataRecord snv = new DataRecord("GHSNV");
		snv.setDataField("SampleId",    "A2049602_1");
		snv.setDataField("RunId",       "160122_NB501062_0070_AHWNNNBGXX");
		snv.setDataField("Gene",        "PIK3CA");
		snv.setDataField("Mutation_AA", "N145N");
		snv.setDataField("Percentage",  14.5);
		snv.setDataField("Chrom",       14);
		snv.setDataField("Position",    178917560L);
		return snv;
	}
	
	@Test
	public void setValueAndGetValueTest() {
		DataRecord snv = setDataRecord();
		
		Assert.assertEquals("A2049602_1",                      snv.getStringVal("SampleId"));
		Assert.assertEquals("160122_NB501062_0070_AHWNNNBGXX", snv.getStringVal("RunId"));
		Assert.assertEquals("PIK3CA",                          snv.getStringVal("Gene"));
		Assert.assertEquals("N145N",                           snv.getStringVal("Mutation_AA"));
		Assert.assertEquals((Double) 14.5,                     snv.getDoubleVal("Percentage"));
		Assert.assertEquals((Integer) 14,                      snv.getIntegerVal("Chrom"));
		Assert.assertEquals((Long) 178917560L,                 snv.getLongVal("Position"));
	}
	
	@Test
	public void getValueFieldsTest() {
		DataRecord snv = setDataRecord();
		Map<String, Object> valueMap = snv.getValueFields();
		
		assertThat(valueMap, IsMapContaining.hasEntry("SampleId",    "A2049602_1"));
		assertThat(valueMap, IsMapContaining.hasEntry("RunId",       "160122_NB501062_0070_AHWNNNBGXX"));
		assertThat(valueMap, IsMapContaining.hasEntry("Gene",        "PIK3CA"));
		assertThat(valueMap, IsMapContaining.hasEntry("Mutation_AA", "N145N"));
		assertThat(valueMap, IsMapContaining.hasEntry("Percentage",  14.5));
		assertThat(valueMap, IsMapContaining.hasEntry("Chrom",       14));
		assertThat(valueMap, IsMapContaining.hasEntry("Position",    178917560L));
	}
	
	@Test
	public void getTypeFieldsTest() {
		DataRecord snv = setDataRecord();
		Map<String, Class<?>> typeMap = snv.getTypeFields();
		
		assertThat(typeMap, IsMapContaining.hasEntry("SampleId",    String.class));
		assertThat(typeMap, IsMapContaining.hasEntry("RunId",       String.class));
		assertThat(typeMap, IsMapContaining.hasEntry("Gene",        String.class));
		assertThat(typeMap, IsMapContaining.hasEntry("Mutation_AA", String.class));
		assertThat(typeMap, IsMapContaining.hasEntry("Percentage",  Double.class));
		assertThat(typeMap, IsMapContaining.hasEntry("Chrom",       Integer.class));
		assertThat(typeMap, IsMapContaining.hasEntry("Position",    Long.class));
	}
	
	@Test
	public void getDataTypeNameTest() {
		DataRecord snv = setDataRecord();
		
		Assert.assertEquals("GHSNV", snv.getDataTypeName());
	}
}
