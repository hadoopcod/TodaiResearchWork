package com.base_stations_CDR_group;

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
		
		int group = key1.getCELID().compareTo(key2.getCELID());
				
		if(group == 0){
		// (check on udid)
			group = key1.getIMEI().compareTo(key2.getIMEI());
			if (group ==0){
				group = key1.getTime().compareTo(key2.getTime());
			}
		}
				
		return group;

	}

}