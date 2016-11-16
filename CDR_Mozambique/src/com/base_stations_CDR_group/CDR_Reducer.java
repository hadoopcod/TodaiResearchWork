package com.base_stations_CDR_group;


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

public class CDR_Reducer extends Reducer< CompositeKey,Text, Text, Text> {

	private static final Log _log = LogFactory.getLog(CDR_Reducer.class);
	

	public void reduce( CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text k = new Text(key.toString());
		
		
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			
			System.out.println("Key" + k);
			System.out.println("ValueA" + v);
			
			
			try{
			
			String[] cdr_data_value = v.toString().split(",");
			context.write(k, v);
			
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
	
	}
	
	
	

		}


