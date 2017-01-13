package com.sort_by_link_id;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Link_Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private static final String String = null;
	private Text imei = new Text();
	//private final Text line_data = new Text();
	//String v0, v1, v2,v3,v4,v5,v6;
	private static final Log _log = LogFactory.getLog(Link_Reducer.class);
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		 
		 String line = value.toString();
							 
		 try { 
			String Link_data_[] = line.split(";");
			String imei = Link_data_[0].trim();
			Long link_id = Long.parseLong(Link_data_[1].trim());
			double speed = Double.parseDouble(Link_data_[2].trim());
			String link = Link_data_[3].trim();
			
			
			Text stockValue = new Text(";"+imei +  ";" + speed+ ";" +link);
			//System.out.println(stockKey +", "+stockValue);
			context.write(new LongWritable(link_id), stockValue);
						
			} catch (Exception e) 
		 { e.printStackTrace(); }
	}

}


