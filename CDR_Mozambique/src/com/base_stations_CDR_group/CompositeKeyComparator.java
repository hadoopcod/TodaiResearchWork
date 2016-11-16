package com.base_stations_CDR_group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class CompositeKeyComparator extends WritableComparator {
protected CompositeKeyComparator() {
super(CompositeKey.class, true);
}
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) {
 
CompositeKey key1 = (CompositeKey) w1;
CompositeKey key2 = (CompositeKey) w2;
 
// (first check on udid)
int compare = key1.getCELID().compareTo(key2.getCELID());
 
if (compare==0) {
	compare = key1.getIMEI().compareTo(key2.getIMEI());
	if(compare ==0){
// only if we are in the same input group should we try and sort by value (datetime)
	compare = key1.getTime().compareTo(key2.getTime());
	
	}
}
 
return compare;

}
}



