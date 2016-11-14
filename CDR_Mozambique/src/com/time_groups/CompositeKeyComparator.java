package com.time_groups;

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
int compare = key1.getHour().compareTo(key2.getHour());
 
if (compare==0) {
// only if we are in the same input group should we try and sort by value (datetime)
	compare = key1.getIMEI().compareTo(key2.getIMEI());
	
	
}
 
return compare;

}
}



