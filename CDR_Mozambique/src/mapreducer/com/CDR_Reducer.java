package mapreducer.com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CDR_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		//System.out.println("Reducer : " + key);
		//List <String> cdr_content = new ArrayList<String>();
		
		/*Iterator<IntWritable> it = values.iterator();
		
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			String[] val = v.toString().split(",");
			int count = Integer.parseInt(val[1]);
			sum +=count;
			//count += values.
			
			//System.out.println("Key" + key);
			//System.out.println("ValueA" + v);
			//Map_Results.put(key.toString(), v.toString());

			//cdr_content.add(v.toString());
		
			
		}
		result.set(sum);
		context.write(key, result);
		//result.set(sum);
			if ((sum >= 2) && (sum <=144)){
			
				for(String x: cdr_content){
				context.write(key,new Text(x));
				}
		}
		
*/	
		for (IntWritable val : values) {
	        sum += val.get();
		}
		 result.set(sum);
		 if ((sum>1) && (sum<30000))
	      context.write(key,result);
	}
}
