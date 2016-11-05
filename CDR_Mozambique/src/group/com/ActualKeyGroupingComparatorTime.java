package group.com;
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

	    int result = ts1.getDatetime().compareTo(ts2.getDatetime());
	    if (result == 0) {
	       result = ts1.getDatetime().compareTo(ts2.getDatetime());
	    }
	    return result;

}
}