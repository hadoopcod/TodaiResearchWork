package join.com;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CDR_BaseStation_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	// Variables to aid the join process
	private String imei_, lac_name, cel1, cel2;
	/*
	 * Map to store Delivery Codes and Messages Key being the status code and
	 * vale being the status message
	 */
	private static Map<String, String> Base_station_Map = new HashMap<String, String>();

	public void configure(JobConf job) {
		// To load the Delivery Codes and Messages into a hash map
		load_Base_station_Map();

	}

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		//System.out.println("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK" + key.toString());
		//System.out.println("VVVVVVVVVTTTTTTTVVVVVVVVTTTTTTVVVVVVTTTTTVVV" + values.toString());
		/*String strRead;
		BufferedReader reader = new BufferedReader(
				new FileReader("/home/cumbane/InputFolder/InputBaseStation/base_station_data"));
		while ((strRead = reader.readLine()) != null) {
			String splitarray[] = strRead.split(",");
			// parse record and load into HahMap
			//System.out.println("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL: " + splitarray[2].trim());
			Base_station_Map = new HashMap<String, String>();
			Base_station_Map.put(splitarray[2].trim(), strRead);
		}
		
		reader.close();
		values.next();*/
		//System.out.println("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK" + key.toString());
		//System.out.println("VVVVVVVVVTTTTTTTVVVVVVVVTTTTTTVVVVVVTTTTTVVV" + values.toString());

		while (values.hasNext()) {
			String currValue = values.next().toString();
			String valueSplitted[] = currValue.split(",");

			//System.out.println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" + currValue);

			if (valueSplitted[0].equals("cdr_data") ) {
				cel1 = valueSplitted[0].trim();
				imei_ = valueSplitted[1].trim();
				//System.out.println(imei_);
				//System.out.println(key.toString());
				
				if(Base_station_Map.containsKey(key.toString()))
				{
					//System.out.println( cel1 + "," +  Base_station_Map.get(key.toString()) );
					
					output.collect( key , new Text( "," + currValue + "," + Base_station_Map.get(key.toString()) ));
				}
				
				//System.out.println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII" + cel1);
			} else if (valueSplitted[0].equals("base_station_data")) {
				// getting the delivery code and using the same to obtain the
				// Message
				//System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" + valueSplitted[3].trim());
				lac_name = Base_station_Map.get(valueSplitted[2].trim());
				
			}
			
			
		}
		
		// pump final output to file
		
		
		/*
		if (imei_ != null && lac_name != null) {
			output.collect(new Text(imei_), new Text(lac_name));
		} else if (imei_ == null) {
			System.out.println("OOOOOOOOOOOOOOOOOO" + lac_name);
			output.collect(new Text("imei_"), new Text(lac_name));
		} else if (lac_name == null)
			output.collect(new Text(imei_), new Text("lac_name"));
		*/
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
}