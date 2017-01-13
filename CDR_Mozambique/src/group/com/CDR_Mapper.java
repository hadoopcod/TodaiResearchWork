package group.com;


	import java.io.IOException;
	import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;
	
	public class CDR_Mapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
		private static final String String = null;
		private Text imei = new Text();
		//private final Text line_data = new Text();
		//String v0, v1, v2,v3,v4,v5,v6;
		private static final Log _log = LogFactory.getLog(CDR_Reducer.class);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			 
			 String line = value.toString();
			 
			 
			 try { 
				 String CDR_data_[] = line.split(",");
				
				 String imei_ = CDR_data_[2].trim();
				//System.out.println(imei_);
				String imsi_ =  CDR_data_[3].trim();
				String s_timestamp = CDR_data_[4].trim();
				//System.out.println(s_timestamp);
				String e_timestamp = CDR_data_[5].trim();
				//Long lac = Long.parseLong(CDR_data_[5].trim());
				String lac = CDR_data_[9].trim();
				String cel_id = CDR_data_[10].trim();
				String activity_type = CDR_data_[7].trim();
				String cel_tower_name = CDR_data_[8].trim();
				String latitude = CDR_data_[11].trim();
				String longitude = CDR_data_[12].trim();
				//System.out.println(latitude);
				
				Long unix_time = DateParser.timeToUnixTime(s_timestamp);
				
				//Integer time_in_minutes = DateParser.timeToMinutes(s_timestamp);
				
				//Integer time_group = com.time_groups.Snippet.TimeMapInMinutesToHour(time_in_minutes);
				//String composite_imei_time_group = imei_ + "_" + time_group.toString();
				
				CompositeKey stockKey = new  CompositeKey(imei_ , unix_time);
				Text stockValue = new Text(","+imsi_ + "," + s_timestamp + "," + e_timestamp+ ","+ activity_type+ "," +lac+ "," +cel_id+ "," +cel_tower_name+ "," +latitude+ "," +longitude);
				//System.out.println(stockKey +", "+stockValue);
				context.write(stockKey, stockValue);
				_log.debug(stockKey.toString() + " => " + stockValue.toString());
				
			 } catch (Exception e) 
			 { e.printStackTrace(); }
		}

}
