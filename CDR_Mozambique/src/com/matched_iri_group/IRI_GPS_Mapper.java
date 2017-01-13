package com.matched_iri_group;


	import java.io.IOException;
	import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;
	
	public class IRI_GPS_Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static final String String = null;
		private Text imei = new Text();
		private static final Log _log = LogFactory.getLog(IRI_GPS_Reducer.class);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			 
			 String line = value.toString();
			 
			 try { 
				String IRI_GPS_data_[] = line.split(";");
				Long link_id = Long.parseLong(IRI_GPS_data_[4].trim());
				//System.out.println(imei_);
				double iri = Double.parseDouble( IRI_GPS_data_[1].trim());
				String line_string =  IRI_GPS_data_[5].trim();
				
				Text stockValue = new Text(";"+iri+";"+line_string);
				
				context.write(new LongWritable(link_id), stockValue);
			//_log.debug(imei_.toString() + " => " + occupancy.toString());
				
				
				
			
			 } catch (Exception e) 
			 { e.printStackTrace(); }
		}

}
