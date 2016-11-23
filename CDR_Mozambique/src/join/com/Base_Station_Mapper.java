package join.com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Base_Station_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	// variables to process delivery report
	private String cel_id, lac_name, fileTag = "base_station_data";
	String lac_id;
	/*
	 * map method that process DeliveryReport.txt and frames the initial key
	 * value pairs Key(Text) – mobile number Value(Text) – An identifier to
	 * indicate the source of input(using ‘DR’ for the delivery report file) +
	 * Status Code
	 */

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			// taking one line/record at a time and parsing them into key value
			// pairs
			String line = value.toString();

			// System.out.println(line);

			String base_station[] = line.split(",");

			// System.out.println(base_station.length );

			if (base_station.length == 5) {
				if (base_station[0].equals("")) {
					lac_name = "N/A";
				} else {
					lac_name = base_station[0].trim();
				}
				if (base_station[1].trim().equals("")) {
					lac_id = "N/A";
				} else {
					lac_id = base_station[1].trim();
				}
				cel_id = base_station[2].trim();
				double latitude = Double.parseDouble(base_station[3].trim());
				double longitude = Double.parseDouble(base_station[4].trim());

				output.collect(new Text(cel_id), new Text(
						fileTag + "," + lac_name + "," + lac_id + "," + cel_id + "," + latitude + "," + longitude));

			}

			// sending the key value pair out of mapper
			// output.collect(new Text(cel_id), new
			// Text(fileTag+lac_name+latitude+longitude));

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("main ");
		}
	}
}