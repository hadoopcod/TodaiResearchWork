package mapreducer.com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CDR_Count {

	public static void main(String[] args) throws Exception {
		try {
			Configuration conf = new Configuration();
			conf.set("mapred.textoutputformat.separator", ",");
			Job job = Job.getInstance(conf, "CDR Count");
			
			job.setJarByClass(CDR_Count.class);
			job.setMapperClass(CDR_Mapper.class);
			job.setCombinerClass(CDR_Reducer.class);
			job.setReducerClass(CDR_Reducer.class);
			conf.set("mapreduce.cluster.local.dir", "/home/cumbane/Cache");
		    //job.setInputFormatClass(TextInputFormat.class);
	        //job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem fs = FileSystem.newInstance(conf);

			if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("main ");

		}
	}
}
