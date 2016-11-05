package join1;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CDR_Base_Station_Mapper extends
 Reducer<Text, NullWritable, Text, NullWritable> {
@Override
public void reduce(Text key, Iterable<NullWritable> values, Context con) {
 try {
  con.write(key, NullWritable.get());
 } catch (IOException e) {
  e.printStackTrace();
 } catch (InterruptedException e) {
  e.printStackTrace();
 }
}
}


