package join.com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CDR_Data_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String cel_id, imei_, fileTag = "cdr_data";

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String CDR_data_[] = line.split(",");
		imei_ = CDR_data_[0].trim();
		String imsi_ = CDR_data_[2].trim();
		String s_timestamp = CDR_data_[4].trim();
		String e_timestamp = CDR_data_[5].trim();
		cel_id = CDR_data_[7].trim();
		String activity_type = CDR_data_[10].trim();
		// sending the key value pair out of mapper
		// output.collect(new Text(cel_id), new
		// Text(fileTag+imei_+imsi_+s_timestamp+e_timestamp+lac+activity_type));
		//System.out.println(line);
		
		context.write(new Text(cel_id), new Text(fileTag + "," + imei_ + "," + imsi_ + "," + s_timestamp + "," + e_timestamp + "," + cel_id + "," + activity_type));
	}

}



/*import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import group.com.CompositeKey;

public class CDR_Data_Mapper implements Mapper<LongWritable, Text, Text, Text>{
	// variables to process Consumer Details
	private String cel_id, imei_, fileTag = "cdr_data";
	//private MultipleOutputs<Text, Text> multipleOutputs; 
	
	 * map method that process ConsumerDetails.txt and frames the initial key
	 * value pairs Key(Text) – mobile number Value(Text) – An identifier to
	 * indicate the source of input(using ‘CD’ for the customer details file) +
	 * Customer Name
	 
	//OutputCollector<Text, Text> output, Reporter reporter
	@SuppressWarnings("unchecked")
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try{
		
			// taking one line/record at a time and parsing them into key value
			// pairs
			String line = value.toString();
			String CDR_data_[] = line.split(",");
			imei_ = CDR_data_[0].trim();
			String imsi_ = CDR_data_[2].trim();
			String s_timestamp = CDR_data_[4].trim();
			//System.out.println("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS: "+s_timestamp);
			//System.out.println(s_timestamp);
			
			String e_timestamp = CDR_data_[5].trim();
			//System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: "+e_timestamp);
			//Long lac = Long.parseLong(CDR_data_[5].trim());
			cel_id = CDR_data_[7].trim();
			//System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC: "+cel_id);
			String activity_type = CDR_data_[10].trim();
			//System.out.println("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC: "+cel_id);
			// sending the key value pair out of mapper
			// output.collect(new Text(cel_id), new
			// Text(fileTag+imei_+imsi_+s_timestamp+e_timestamp+lac+activity_type));
			
			//String date = DateParser.yearMonthDay(s_timestamp);
			//multipleOutputs.write(new Text(cel_id), new Text(fileTag + "," + imei_+ "," + imsi_+ "," + s_timestamp+ "," + e_timestamp+ "," + cel_id+ "," +activity_type), generateFileName(new Text(date)));
			
			//context.write(new Text(cel_id), new Text(fileTag + "," + imei_+ "," + imsi_+ "," + s_timestamp+ "," + e_timestamp+ "," + cel_id+ "," +activity_type));
			output.collect(new Text(cel_id), new Text(fileTag + "," + imei_+ "," + imsi_+ "," + s_timestamp+ "," + e_timestamp+ "," + cel_id+ "," +activity_type));
			
			
		}catch (IOException e) {
			e.printStackTrace();}
		
	}
	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	
}*/
	/*String generateFileName(Text d){
		return d.toString() ;		
	}
	@Override
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}*/
/*	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}*/