
package com.matched_iri_group;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IRI_GPS_Reducer extends Reducer< LongWritable,Text, LongWritable, Text> {
	//private Text result = new Text();

	private static final Log _log = LogFactory.getLog(IRI_GPS_Reducer.class);

	public void reduce( LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Long k = new Long(key.toString());
		int count = 0;
		
		
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			
			
			try{
			
			String[] cdr_data_value = v.toString().split(",");
			
		
			context.write(new LongWritable(k),v);
			//_log.debug(k.toString() + " => " + values.toString());
   			count++;
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	
	
	}
	
}