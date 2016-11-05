package join.com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CDR_Data_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	// variables to process Consumer Details
	private String cel_id, imei_, fileTag = "cdr_data";

	/*
	 * map method that process ConsumerDetails.txt and frames the initial key
	 * value pairs Key(Text) – mobile number Value(Text) – An identifier to
	 * indicate the source of input(using ‘CD’ for the customer details file) +
	 * Customer Name
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			// taking one line/record at a time and parsing them into key value
			// pairs
			String line = value.toString();
			String CDR_data_[] = line.split(",");
			imei_ = CDR_data_[0].trim();
			String imsi_ = CDR_data_[2].trim();
			String s_timestamp = CDR_data_[3].trim();
			System.out.println(s_timestamp);
			String e_timestamp = CDR_data_[4].trim();
			//Long lac = Long.parseLong(CDR_data_[5].trim());
			cel_id = CDR_data_[6].trim();
			String activity_type = CDR_data_[7].trim();

			// sending the key value pair out of mapper
			// output.collect(new Text(cel_id), new
			// Text(fileTag+imei_+imsi_+s_timestamp+e_timestamp+lac+activity_type));
			output.collect(new Text(cel_id), new Text(fileTag + "," + imei_+ "," + imsi_+ "," + s_timestamp+ "," + e_timestamp+ "," + cel_id+ "," +activity_type));
		
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("main ");
		}
	}
}
