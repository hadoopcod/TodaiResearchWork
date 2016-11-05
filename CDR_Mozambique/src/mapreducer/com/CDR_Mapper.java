package mapreducer.com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CDR_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text imei = new Text();
	private final IntWritable count = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		CDR_Data record = new CDR_Data(value.toString());

		try {

			String imei_ = record.imei_caller;
			String imsi_ = record.imsi_caller;

			//System.out.println(imei_);

			imei.set(imei_);
			count.set(1);
			context.write(imei, count);

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
