package com.matched_iri_group;

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

public class IRI_GPS_group {

	public static void main(String[] args) throws Exception {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "GPS_group");
			
			job.setJarByClass(IRI_GPS_group.class);
			job.setMapperClass(IRI_GPS_Mapper.class);
			//job.setCombinerClass(CDR_Reducer.class);
			job.setReducerClass(IRI_GPS_Reducer.class);
			conf.set("mapreduce.textoutputformat.separatorText", ",");
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
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
