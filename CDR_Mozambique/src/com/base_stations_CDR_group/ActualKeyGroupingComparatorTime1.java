package com.base_stations_CDR_group;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 

public class ActualKeyGroupingComparatorTime extends WritableComparator {
	protected ActualKeyGroupingComparatorTime(){
		 super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey ts1 = (CompositeKey)a;
		CompositeKey ts2 = (CompositeKey)b;

	    int result = ts1.getCELID().compareTo(ts2.getCELID());
	    if (result == 0) {
	       result = ts1.getCELID().compareTo(ts2.getCELID());
	    }
	    return result;

}
}