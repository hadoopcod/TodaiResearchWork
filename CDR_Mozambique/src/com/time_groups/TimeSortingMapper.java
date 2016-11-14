package com.time_groups;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.time_groups.TimeShortingReducer;
import com.time_groups.CompositeKey;
import com.time_groups.DateParser;

public class TimeSortingMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	private static final String String = null;
	private Text imei = new Text();
	private IntWritable h = new IntWritable();
	//private final Text line_data = new Text();
	//String v0, v1, v2,v3,v4,v5,v6;
	private static final Log _log = LogFactory.getLog(TimeShortingReducer.class);
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		 
		 String line = value.toString();
		 
			
			//StockKey stockKey = new StockKey(symbol, timestamp);
			//DoubleWritable stockValue = new DoubleWritable(v);
			
			//context.write(stockKey, stockValue);
			//_log.debug(stockKey.toString() + " => " + stockValue.toString());
		
		 //System.out.println(line); 
		 
		 
		 try { 
			 String CDR_data_[] = line.split(",");
			String imei_ = CDR_data_[0].trim();
			//System.out.println(imei_);
			String imsi_ =CDR_data_[2].trim();
			String s_timestamp = CDR_data_[3].trim();
			//System.out.println(s_timestamp);
			String e_timestamp = CDR_data_[4].trim();
			Long lac = Long.parseLong(CDR_data_[5].trim());
			String cel_id = CDR_data_[6].trim();
			//String activity_type = CDR_data_[6].trim();
			String cel_tower_name = CDR_data_[7].trim();
			String latitude = CDR_data_[8].trim();
			String longitude = CDR_data_[9].trim();
			//System.out.println(latitude);
			
			//Integer unix_time = DateParser.timeToMinutes(s_timestamp);
			//System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC: "+unix_time);
			Integer hour = DateParser.timeToMinutes(s_timestamp);
			Integer h = Snippet.TimeMapInMinutesToHour(hour);
			
			//imei.set(imei_);
			//h.set(hour);
			System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC: "+h +";"+imei_);
			 CompositeKey stockKey = new  CompositeKey(h, imei_);
			Text stockValue = new Text("," +imsi_ + "," + s_timestamp + "," + e_timestamp+ "," +lac+ "," +cel_id+ "," +cel_tower_name+ "," +latitude+ "," +longitude);
			//System.out.println(stockKey +", "+stockValue);
			context.write(stockKey, stockValue);
			_log.debug(stockKey.toString() + " => " + stockValue.toString());
			
		 } catch (Exception e) 
		 { e.printStackTrace(); }
	}

}









