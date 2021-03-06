package group.com;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator extends WritableComparator {

	protected ActualKeyGroupingComparator() {

		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;
		
		int group = key1.getUDID().compareTo(key2.getUDID());
		
		
		if(group == 0){
		// (check on udid)
			group = key1.getDatetime().compareTo(key2.getDatetime());
		}
				
		return group;

	}

}