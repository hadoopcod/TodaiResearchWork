
package group.com;

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

public class CDR_Reducer extends Reducer< CompositeKey,Text, Text, Text> {
	//private Text result = new Text();

	private static final Log _log = LogFactory.getLog(CDR_Reducer.class);

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
						
			try{
			
			String[] cdr_data_value = v.toString().split(",");
	
			context.write(k, v);
			//_log.debug(k.toString() + " => " + values.toString());
   			count++;
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		//_log.debug("count = " + count);
	
	}
	
	
	// To load the Delivery Codes and Messages into a hash map
		/*private void load_Base_station_Map() {
			String strRead;
			try {
				// read file from Distributed Cache
				@SuppressWarnings("resource")
				BufferedReader reader = new BufferedReader(
						new FileReader("/home/cumbane/InputFolder/InputBaseStation/base_station_data"));
				while ((strRead = reader.readLine()) != null) {
					String splitarray[] = strRead.split(",");
					// parse record and load into HahMap
					System.out.println("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL" + strRead);

					Base_station_Map.put(splitarray[2].trim(), strRead);

				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();*/
			
				
			

		}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*public void reduce(Text key, Text values, Context context)
			throws IOException, InterruptedException {
		//int sum = 0;
		

		System.out.println("Reducer : " + values);

		for (IntWritable val : values) {
			sum += val.get();
		}
		//key.set(imei);
		//String res =CDR_Mapper.obj;
		result.set(values.toString());
		context.write(key, result);*/


