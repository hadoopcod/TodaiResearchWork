package join.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Join_CDR_BS {

	public static void main(String[] args) throws Exception {
		// CDRDriver join_cdr_bs = new CDRDriver();
		// int res = ToolRunner.run(join_cdr_bs, args);
		// System.exit(res);

		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("join_cdr_bs");
		
	
		//job.setOutputKeyClass(LongWritable.class);
		//job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(CDR_Data_Mapper.class);
		job.setCombinerClass(CDR_BaseStation_Reducer.class);
		job.setReducerClass(CDR_BaseStation_Reducer.class);
		
		conf.set("mapreduce.cluster.local.dir", "/home/cumbane/Cache");
		
		//job.setJarByClass(CDRDataReducer.class);


		job.setInputFormatClass(TextInputFormat.class);

		/*
		 * Using MultipleOutputs creates zero-sized default output Ex:
		 * part-r-00000. To prevent this use LazyOutputFormat
		 */
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		/* This line is to accept input recursively */
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		/*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.waitForCompletion(true);

	}
}







/*
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import join.com.CDR_Data_Mapper;

import org.apache.hadoop.util.Tool;
*/

/*public class Join_CDR_BS extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
		
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Join_CDR_BS");
			job.setJarByClass(Join_CDR_BS.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			//job.setMapOutputKeyClass(CDR_Data_Mapper.class);
			//job.setCombinerClass(WordcountReducer.class);
			//job.setReducerClass(WordcountReducer.class);
			
			
			//job.setJarByClass(CDR_group.class);
			job.setMapperClass(CDR_Data_Mapper.class);
			job.setCombinerClass(CDR_BaseStation_Reducer.class);
			job.setReducerClass(CDR_BaseStation_Reducer.class);
			//job.setInputFormatClass(TextInputFormat.class);
			//job.setOutputFormatClass(TextOutputFormat.class);
			
			//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CDR_Data_Mapper.class);
			//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			//MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,Text.class, IntWritable.class);
			//MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, Text.class, Text.class);
			
			Path inputFilePath = new Path(args[0]);
			Path outputFilePath = new Path(args[1]);

			 This line is to accept input recursively 
			FileInputFormat.setInputDirRecursive(job, true);
			
			FileInputFormat.addInputPath(job, inputFilePath);
			FileOutputFormat.setOutputPath(job, outputFilePath);
			
			FileSystem fs = FileSystem.newInstance(getConf());

			if (fs.exists(outputFilePath)) {
				fs.delete(outputFilePath, true);
			}
			return job.waitForCompletion(true) ? 0: 1;	
			
		} 
	
	public static void main(String[] args) throws Exception {
		Join_CDR_BS join_CDR_BS = new Join_CDR_BS();
		int res = ToolRunner.run(join_CDR_BS, args);
		System.exit(res);
	}
}
*/


/*public class Join_CDR_BS extends Configured implements Tool
{
	
	
	
	public int run(String[] args) throws Exception {
		
			
             //get the configuration parameters and assigns a job name
             JobConf conf = new JobConf(getConf(), Join_CDR_BS.class);
             conf.setJobName("Join_CDR_BS");
             
             //setting key value types for mapper and reducer outputs
             conf.setOutputKeyClass(Text.class);
             conf.setOutputValueClass(Text.class);

             //specifying the custom reducer class
             //conf.setMapperClass(CDR_Data_Mapper.class);
             //conf.setCombinerClass(CDR_BaseStation_Reducer.class);
             conf.setReducerClass(CDR_BaseStation_Reducer.class);
             
            // LazyOutputFormat.setOutputFormatClass(conf, TextOutputFormat.class);
     		 //MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,Text.class, Text.class);
             //Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
             //FileSystem fs = FileSystem.get(new Configuration());
       	  	 //int number_of_companies = fs.listStatus(new Path(args[0])).length;
             //FileInputFormat.setInputDirRecursive(conf, true);
             MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, CDR_Data_Mapper.class);
             //MultipleInputs.addInputPath(conf, new Path(args[0]), CDR_Data_Mapper.class);
             //MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Base_Station_Mapper.class);
            
             //Specifying the output directory @ runtime
             //FileOutputFormat.setOutputDirRecursive(conf, true);
             LazyOutputFormat.setOutputFormatClass(conf, TextOutputFormat.class);
             //MultipleOutputs.addMultiNamedOutput(conf, "seq",TextOutputFormat.class, Text.class, Text.class);
             FileOutputFormat.setOutputPath(conf, new Path(args[1]));
             
             FileSystem fs = FileSystem.newInstance(getConf());
 				if (fs.exists(new Path(args[1]))) {
 				fs.delete(new Path(args[1]), true);
 				}
 				
             JobClient.runJob(conf);
             return 0;
            
	}
   
	public static void main(String[] args) throws Exception {
		Join_CDR_BS join_cdr_bs = new Join_CDR_BS();
		int res = ToolRunner.run(join_cdr_bs, args);
		System.exit(res); 
	

      public static void main(String[] args) throws Exception {
    		
             int res = ToolRunner.run(new Configuration(), new Join_CDR_BS(), args);
             System.exit(res);
          
      
      
      }
}


*/