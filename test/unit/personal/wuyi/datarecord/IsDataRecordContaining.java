package personal.wuyi.datarecord;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;

/**
 * New matcher class to simulate the {@code IsMapContaining} class for testing 
 * {@code DataRecord}.
 * 
 * <p>Because of {@code IsMapContaining} is only applicable to test the 
 * classes which implements the {@code Map} interface. But {@code DataRecord} 
 * didn't implements the {@code Map} interface. And also {@code DataRecord} 
 * doesn't expose generic type, and the type of value is vary per entry. So 
 * the {@code IsMapContaining} can not be used to test {@code DataRecord}.
 * 
 * @author  Wuyi Chen
 * @date    11/29/2018
 * @version 1.2
 * @since   1.2
 *
 * @param <T> The type for value in {@code DataRecord}.
 */
public class IsDataRecordContaining<T> extends BaseMatcher<DataRecord> {
	private final String targetKey;
    private final T      targetValue;
    
    final private Class<?> expectedType = DataRecord.class;     // restrict the type of input object should be DataRecord. 
    
    /**
     * Construct a {@code IsDataRecordContaining}.
     * 
     * @param  key
     *         The key needs to be matched.
     *         
     * @param  value
     *         The value needs to be matched.
     *         
     * @since   1.2
     */
    public IsDataRecordContaining(String key, T value) {
        this.targetKey = key;
        this.targetValue = value;
    }

	@Override
	public boolean matches(Object obj) {
		return obj != null                              // check input object is not null
                && expectedType.isInstance(obj)         // check input object is a DataRecord
                && matchesSafely((DataRecord) obj);     // check the DataRecord matches a certain criteria
	}
	
	/**
	 * Customize the matching criteria for {@code DataRecord}.
	 * 
	 * <p>This method will check a {@code DataRecord} has a certain key-value 
	 * pair or not, and the type of the value is matched.
	 * 
	 * @param  record
	 *         The input {@code DataRecord} needs to be verified or not.
	 * 
	 * @return  {@code true} if the {@code DataRecord} has at least one certain 
	 *                       key-value pair;
	 *          {@code false} otherwise;
	 *          
     * @since   1.2
	 */
	protected boolean matchesSafely(DataRecord record) {
		Class<?> valueType = record.getTypeFields().get(targetKey);
		Object   valueObj  = record.getValueFields().get(targetKey);
		
		if (valueType == null || valueObj == null) {
			return false;
		} else {
			return targetValue.equals(valueType.cast(valueObj));
		}
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("DataRecord containing [")
			.appendText(targetKey)
			.appendText("->")
			.appendText(targetValue.toString())
			.appendText("]");
		
	}
	
	/**
	 * Creates a matcher for {@code DataRecord} matching when the examined 
     * {@code DataRecord} contains at least one entry whose key equals the 
     * specified <code>key</code> <b>and</b> whose value equals the specified 
     * <code>value</code>.
     * 
     * <p>For example:
     * <pre>
     *     assertThat(myDataRecord, IsDataRecordContaining.hasEntry("bar", "foo"))
     * </pre>
	 * 
	 * @param  key
	 *         The key that, in combination with the value, must be describe 
     *         at least one entry.
	 * 
	 * @param  value
	 *         The value that, in combination with the key, must be describe 
     *         at least one entry.
	 * 
	 * @return  The instance of the matcher for {@code DataRecord}.
	 * 
     * @since   1.2
	 */
	@Factory
    public static <T> IsDataRecordContaining<T> hasEntry(String key, T value) {
        return new IsDataRecordContaining<T>(key, value);
    }
}
