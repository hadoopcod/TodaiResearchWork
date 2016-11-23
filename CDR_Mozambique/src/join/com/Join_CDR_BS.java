package join.com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.mapred.lib.MultipleInputs;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htrace.Trace;


public class Join_CDR_BS extends Configured implements Tool
{

	public int run(String[] args) throws Exception {

             //get the configuration parameters and assigns a job name
             JobConf conf = new JobConf(getConf(), Join_CDR_BS.class);
             conf.setJobName("Join_CDR_BS");

             //setting key value types for mapper and reducer outputs
             conf.setOutputKeyClass(Text.class);
             conf.setOutputValueClass(Text.class);

             //specifying the custom reducer class
             conf.setReducerClass(CDR_BaseStation_Reducer.class);

             //Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
             //FileSystem fs = FileSystem.get(new Configuration());
       	  	 //int number_of_companies = fs.listStatus(new Path(args[0])).length;
             MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, CDR_Data_Mapper.class);
             //MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Base_Station_Mapper.class);
            
             //Specifying the output directory @ runtime
             FileOutputFormat.setOutputPath(conf, new Path(args[1]));

             JobClient.runJob(conf);
             return 0;
            
	}
   
      
      public static void main(String[] args) throws Exception {
    		
             int res = ToolRunner.run(new Configuration(), new Join_CDR_BS(), args);
             System.exit(res);
             
      
      
      
      }
}



