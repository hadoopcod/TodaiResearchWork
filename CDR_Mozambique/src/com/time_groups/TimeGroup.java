package com.time_groups;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.time_groups.ActualKeyGroupingComparator;
import com.time_groups.ActualKeyPartitioner;
import com.time_groups.TimeSortingMapper;
import com.time_groups.TimeShortingReducer;
import com.time_groups.TimeGroup;
import com.time_groups.CompositeKey;
import com.time_groups.CompositeKeyComparator;

public class TimeGroup {

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
			
			
			job.setJarByClass(TimeGroup.class);
			job.setMapperClass(TimeSortingMapper.class);
			//job.setCombinerClass(CDR_Reducer.class);
			job.setReducerClass(TimeShortingReducer.class);
			
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




