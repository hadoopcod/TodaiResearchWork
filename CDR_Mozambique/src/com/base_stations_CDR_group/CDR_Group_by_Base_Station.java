package com.base_stations_CDR_group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.base_stations_CDR_group.ActualKeyGroupingComparator1;
import com.base_stations_CDR_group.ActualKeyPartitioner1;
import com.base_stations_CDR_group.CDR_Mapper;
import com.base_stations_CDR_group.CDR_Reducer;
import com.base_stations_CDR_group.CDR_Group_by_Base_Station;
import com.base_stations_CDR_group.CompositeKey;
import com.base_stations_CDR_group.CompositeKeyComparator;

public class CDR_Group_by_Base_Station {

	public static void main(String[] args) throws Exception {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "CDR_Group_by_Base_Station");
			
			job.setMapOutputKeyClass(CompositeKey.class);
			job.setPartitionerClass(ActualKeyPartitioner1.class);
			job.setGroupingComparatorClass(ActualKeyGroupingComparator1.class);
			//job.setGroupingComparatorClass(ActualKeyGroupingComparatorTime.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);
			
			
			/*job.setPartitionerClass(NaturalKeyPartitioner.class);
			job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);*/
			
			/*job.setMapOutputKeyClass(StockKey.class);
			job.setMapOutputValueClass(Text.class);*/
			
			
			job.setJarByClass(CDR_Group_by_Base_Station.class);
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




