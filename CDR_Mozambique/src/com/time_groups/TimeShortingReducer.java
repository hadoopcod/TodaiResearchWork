package com.time_groups;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.time_groups.TimeShortingReducer;
import com.time_groups.CompositeKey;

public class TimeShortingReducer extends Reducer< CompositeKey,Text, Text, Text> {
	//private Text result = new Text();
	private static final Log _log = LogFactory.getLog(TimeShortingReducer.class);
	
	/*private static Map<String, String> Base_station_Map = new HashMap<String, String>();

	public void configure(JobConf job) {
	
		load_Base_station_Map();

	}
	*/
	public void reduce( CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text k = new Text(key.toString());
		int count = 0;
		
		
		
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			
			//System.out.println("Key" + k);
			//System.out.println("Value" + v);
			
			String[] cdr_data_value = values.toString().split(",");
			//String id = cdr_data_value[0];
			//System.out.println(id);
			
			/*if(Base_station_Map.containsKey(id))
			{
				System.out.println( id + "," +  Base_station_Map.get(id) );
				
				context.write( k , new Text( v + "," + Base_station_Map.get(id) ));
			}*/
			
			context.write(k, v);
			//_log.debug(k.toString() + " => " + values.toString());
   			count++;
		}
		
		//_log.debug("count = " + count);
	
	}
}


