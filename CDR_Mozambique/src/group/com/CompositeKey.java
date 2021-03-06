package group.com;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This key is a composite key. The "actual" key is the UDID. The secondary sort
 * will be performed against the datetime.
 */
@SuppressWarnings("rawtypes")
public class CompositeKey implements WritableComparable {
	//private Integer timegroup;
	private String udid;
	private Long datetime;

	public CompositeKey() {
	}

	public CompositeKey(String udid, Long datetime) {

		//this.timegroup = timegroup;
		this.udid = udid;
		this.datetime = datetime;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(udid).append(',').append(datetime).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		//timegroup = WritableUtils.readVInt(in);
		udid = WritableUtils.readString(in);
		datetime = WritableUtils.readVLong(in);
		// datetime = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		//WritableUtils.writeVInt(out, timegroup);
		WritableUtils.writeString(out, udid);
		WritableUtils.writeVLong(out, datetime);
		// out.writeLong(datetime);
	}

	// @Override
	public int compareTo(CompositeKey o) {
		int result = udid.compareTo(o.udid);
		if (0 == result) {
		result =datetime.compareTo(o.datetime);
		}
		return result;
		}
		/*//int result = timegroup.compareTo(o.timegroup);
		if(result != 0)
			return result;
		//if(result == 0){

		result = udid.compareTo(o.udid);
		if(result != 0)
			return result;
		
		return datetime.compareTo(o.datetime);*/
		
		/*
		if (0 == result) {
			result = datetime.compareTo(o.datetime);
		}
		//}
		return result;
		*/
	

	
	
	
	/*public Integer getTimegroup() {
		return timegroup;
	}

	public void setTimegroup(Integer timegroup) {
		this.timegroup = timegroup;
	}*/

	/**
	 * Gets the udid.
	 *
	 * @return UDID.
	 */
	public String getUDID() {

		return udid;
	}

	public void setUDID(String udid) {

		this.udid = udid;
	}

	/**
	 * Gets the datetime.
	 *
	 * @return Datetime
	 */
	public Long getDatetime() {

		return datetime;
	}

	public void setDatetime(Long datetime) {

		this.datetime = datetime;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}