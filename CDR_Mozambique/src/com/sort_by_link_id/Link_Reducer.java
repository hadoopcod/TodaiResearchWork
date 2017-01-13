package com.sort_by_link_id;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Link_Reducer extends Reducer< LongWritable,Text, LongWritable, Text> {
	//private Text result = new Text();
	private static final Log _log = LogFactory.getLog(Link_Reducer.class);
	
	
	public void reduce( LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//LongWritable k = new LongWritable(key);
		int count = 0;
		
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			
			//String[] cdr_data_value = values.toString().split(",");
			
			context.write(key, v);
			
		}
		
		//_log.debug("count = " + count);
	
	}
}
