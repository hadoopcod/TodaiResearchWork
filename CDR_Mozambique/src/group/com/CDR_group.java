package group.com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
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

public class CDR_group {

	public static void main(String[] args) throws Exception {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "CDR_group");
			
			job.setMapOutputKeyClass(CompositeKey.class);
			job.setPartitionerClass(ActualKeyPartitioner.class);
			job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
			//job.setGroupingComparatorClass(ActualKeyGroupingComparatorTime.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);
			
			
			/*job.setPartitionerClass(NaturalKeyPartitioner.class);
			job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);*/
			
			/*job.setMapOutputKeyClass(StockKey.class);
			job.setMapOutputValueClass(Text.class);*/
			
			
			job.setJarByClass(CDR_group.class);
			job.setMapperClass(CDR_Mapper.class);
			//job.setCombinerClass(CDR_Reducer.class);
			job.setReducerClass(CDR_Reducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("main ");

		}
	}
}
