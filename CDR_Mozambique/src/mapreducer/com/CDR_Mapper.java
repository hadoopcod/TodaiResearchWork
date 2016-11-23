package mapreducer.com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CDR_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text imei = new Text();
	private final IntWritable count = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		CDR_Data record = new CDR_Data(value.toString());

		try {

			String imei_ 		= record.imei_caller;
			String imsi_		= record.imsi_caller;
			String s_timestamp 	= record.Start_timestamp;
			String e_timestamp 	= record.end_timestamp;
			Long lac 			= record.lac;
			String cel_id 		= record.cell_id;
			String cel_name		= record.cel_name;
			Double latitude		= record.latitude;
			Double longitude	= record.longitude; 
			

			//System.out.println(imei_);

			imei.set(imei_);
			count.set(1);
			Text stock_value = new Text(","+count+","+imsi_+","+s_timestamp+","+e_timestamp+","+lac+","+cel_id+","+cel_name+","+latitude+","+longitude);
			context.write(imei,stock_value);

			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

/*
 *
 *
 * CDR_Data d = new CDR_Data(); // CDR_Data data = d.data(value.toString()); //
 * System.out.println(data.imei_caller);
 * 
 * String line = value.toString(); //System.out.println(line); try { String
 * CDR_data_[] = line.split(",");
 * 
 * String imei_ = CDR_data_[0]; String imsi_ = CDR_data_[1];
 * 
 * // while (s.hasMoreTokens()) {
 * 
 * //System.out.println(imei_);
 * 
 * imei.set(imei_); count.set(1); context.write(imei, count);
 * 
 * // } } catch (Exception e) { e.printStackTrace(); }
 * 
 */
