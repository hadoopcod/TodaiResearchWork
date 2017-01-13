package join.com;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//import com.gpstaxi.utility.DateParser;

public class CDR_BaseStation_Reducer extends Reducer<Text, Text, Text, Text> {

	// Variables to aid the join process
	private String imei_, lac_name, cel1, day;
	private MultipleOutputs<Text, Text> multipleOutputs;
	private static Map<String, String> Base_station_Map = new HashMap<String, String>();


	@Override
	public void reduce(Text key, Iterable<Text> values, final Context context) {

		if(Base_station_Map.isEmpty()){
			load_Base_station_Map();
		}
		
		Iterator<Text> iterator = values.iterator();
		// String keyword = statisticParsedLog.getKeyword();
		//System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" + key);
		while (iterator.hasNext()) {
			try {
				String currValue = iterator.next().toString();
				String valueSplitted[] = currValue.split(",");

				// System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" +
				// currValue);

				if (valueSplitted[0].equals("cdr_data")) {
					cel1 = valueSplitted[0].trim();
					imei_ = valueSplitted[1].trim();
					day = valueSplitted[3].trim();
					String date = DateParser.yearMonthDay(day);
					//System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" + date);
					// System.out.println(imei_);
					// System.out.println(key.toString());

					if (Base_station_Map.containsKey(key.toString())) {
						// System.out.println( cel1 + "," +
						// Base_station_Map.get(key.toString()) );

						multipleOutputs.write(key, new Text("," + currValue + "," + Base_station_Map.get(key.toString())), generateFileName(new Text(date)));
						// output.collect( key , new Text( "," + currValue + ","
						// + Base_station_Map.get(key.toString()) ));
					}

					// System.out.println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"
					// + cel1);
				} else if (valueSplitted[0].equals("base_station_data")) {

					lac_name = Base_station_Map.get(valueSplitted[2].trim());

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	String generateFileName(Text name) {
		return name.toString();
	}

	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}

	public void cleanup(final Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	// To load the Delivery Codes and Messages into a hash map
	private void load_Base_station_Map() {
		String strRead;
		try {
			// read file from Distributed Cache
			BufferedReader reader = new BufferedReader(new FileReader("/home/cumbane/InputFolder/BaseStation_Data/base_station_data_filter"));
			while ((strRead = reader.readLine()) != null) {
				String splitarray[] = strRead.split(",");
				// parse record and load into HahMap
				// System.out.println("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL" +
				// splitarray[2].trim());

				Base_station_Map.put(splitarray[2].trim(), strRead);

			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}


/*import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CDR_BaseStation_Reducer implements Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> multipleOutputs;
	// Variables to aid the join process
	private String cel1, imei_;
	
	private static Map<String, String> Base_station_Map = new HashMap<String, String>();

	public void configure(JobConf job) {
		// To load the Delivery Codes and Messages into a hash map
		load_Base_station_Map();

	}
	@SuppressWarnings("unchecked")
	public void reduce( Text key,  Iterator<Text> values,
			final Context context) throws IOException, InterruptedException, ParseException {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		//System.out.println("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK" + key.toString());
		//System.out.println("VVVVVVVVVTTTTTTTVVVVVVVVTTTTTTVVVVVVTTTTTVVV" + values.toString());
		
		while (values.hasNext()) {
			try{
			String currValue = values.next().toString();
			String valueSplitted[] = currValue.split(",");
			
			//String datevalue = valueSplitted[3].trim();
			//System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD: "+ datevalue);
		
			//String date = DateParser.yearMonthDay(datevalue);
			
			//System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" + date);

			if (valueSplitted[0].equals("cdr_data") ) {
				cel1 = valueSplitted[0].trim();
				imei_ = valueSplitted[1].trim();
				
				//System.out.println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"+imei_);
				//System.out.println(key.toString());
				
				if(Base_station_Map.containsKey(key.toString()))
				{
					//System.out.println( cel1 + "," +  Base_station_Map.get(key.toString()) );
					
					//multipleOutputs.write(key, new Text( "," + currValue + "," + Base_station_Map.get(key.toString()) ), generateFileName(new Text(date)));
					output.collect( key , new Text( "," + currValue + "," + Base_station_Map.get(key.toString()) ));
					//context.write(key, new Text( "," + currValue + "," + Base_station_Map.get(key.toString())));
					
					
				}
				
				
				//System.out.println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII" + cel1);
			} else if (valueSplitted[0].equals("base_station_data")) {
				Base_station_Map.get(valueSplitted[2].trim());
				
			}
		
			}
			catch (IOException e) {
				e.printStackTrace();
			} 
		}
				
		}
	

	String generateFileName(Text d){
		return d.toString() ;		
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	@SuppressWarnings("rawtypes")
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
	// To load the Delivery Codes and Messages into a hash map
	
	private void load_Base_station_Map() {
		String strRead;
		try {
			// read file from Distributed Cache
			BufferedReader reader = new BufferedReader(
					new FileReader("/home/cumbane/InputFolder/BaseStation_Data/base_station_data_filter"));
			while ((strRead = reader.readLine()) != null) {
				String splitarray[] = strRead.split(",");
				// parse record and load into HahMap
				//System.out.println("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL" + splitarray[2].trim());

				Base_station_Map.put(splitarray[2].trim(), strRead);

			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	

		}

	
*/