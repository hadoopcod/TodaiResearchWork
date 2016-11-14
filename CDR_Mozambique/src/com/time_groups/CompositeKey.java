package com.time_groups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
 
/**
* This key is a composite key. The "actual"
* key is the UDID. The secondary sort will be performed against the datetime.
*/
@SuppressWarnings("rawtypes")
public class CompositeKey implements WritableComparable{
	private Integer hour;
	private String imei;
 
	public CompositeKey() {
	}
 
	public CompositeKey(Integer hour,String imei) {
 
		this.hour = hour;
		this.imei = imei;
}
 
@Override
public String toString() {
 
return (new StringBuilder()).append(hour).append(',').append(imei).toString();
}
 
@Override
public void readFields(DataInput in) throws IOException {
 
hour = WritableUtils.readVInt(in);
imei = WritableUtils.readString(in);
//datetime = in.readLong();

}
 
@Override
public void write(DataOutput out) throws IOException {
 
WritableUtils.writeVInt(out, hour);
WritableUtils.writeString(out, imei);
//out.writeLong(datetime);
}
 
//@Override
public int compareTo(CompositeKey o) {
 
int result = hour.compareTo(o.hour);
if (0 == result) {
result =imei.compareTo(o.imei);
}
return result;
}
 
/**
* Gets the udid.
*
* @return UDID.
*/
public Integer getHour() {
 
return hour;
}
 
public void setHour(Integer hour) {
 
this.hour = hour;
}
 
/**
* Gets the datetime.
*
* @return Datetime
*/
public String getIMEI() {
 
return imei;
}
 
public void setIMEI(String Imei) {
 
this.imei = imei;
}

@Override
public int compareTo(Object o) {
	// TODO Auto-generated method stub
	return 0;
}
 
}