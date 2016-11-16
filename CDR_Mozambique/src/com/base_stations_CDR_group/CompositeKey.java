		package com.base_stations_CDR_group;
		
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
			private String cel_id;
			private String imei;
			private Long time;
		 
			public CompositeKey() {
			}
		 
			public CompositeKey(String cel_id,String imei, Long time) {
		 
				this.cel_id = cel_id;
				this.imei = imei;
				this.time = time;
		}
		 
		@Override
		public String toString() {
		 
		return (new StringBuilder()).append(cel_id).append(',').append(imei).append(',').append(time).toString();
		}
		 
		@Override
		public void readFields(DataInput in) throws IOException {
		 
		cel_id = WritableUtils.readString(in);
		imei = WritableUtils.readString(in);
		time = WritableUtils.readVLong(in);
		//datetime = in.readLong();
		
		}
		 
		@Override
		public void write(DataOutput out) throws IOException {
		 
		WritableUtils.writeString(out, cel_id);
		WritableUtils.writeString(out, imei);
		WritableUtils.writeVLong(out, time);
		//out.writeLong(datetime);
		}
		 
		//@Override
		public int compareTo(CompositeKey o) {
		 
		int result = cel_id.compareTo(o.cel_id);
		if (0 == result) {
			
		result =imei.compareTo(o.imei);
		if (0 == result){
			result =time.compareTo(o.time);
		}
		}
		return result;
		}
		 
		/**
		* Gets the udid.
		*
		* @return UDID.
		*/
		public String getCELID() {
		 
		return cel_id;
		}
		 
		public void setCELID(String cel_id) {
		 
		this.cel_id = cel_id;
		}
		 
		/**
		* Gets the datetime.
		*
		* @return Datetime
		*/
		public String getIMEI() {
		 
		return imei;
		}
		 
		public void setIMEI(String imei) {
		 
		this.imei = imei;
		}
		
		public Long getTime() {
			return time;
		}

		public void setTime(Long time) {
			this.time = time;
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}
		 
		}