package com.time_groups;


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
 
// (check on udid)
 int group = key1.getHour().compareTo(key2.getHour());
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