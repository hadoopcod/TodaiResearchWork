package join1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CDR_Base_Station_Reducer extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<String, String> deptInputMap = new HashMap<String, String>();

	/* This method is executed for each mapper task before map method */
	protected void setup(Context context) throws IOException, InterruptedException {
		Path path = null;
		try {
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI cachedFileURI = context.getCacheFiles()[0];
				if (cachedFileURI != null) {
					System.out.println("Mapping File: " + cachedFileURI.toString());
					path = new Path(cachedFileURI.toString());
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString())));
					String inputLine = "";

					while ((inputLine = br.readLine()) != null) {
						String[] base_station = inputLine.split(",");
						if (base_station.length == 5) {
							/*
							 * if (base_station[0].equals("")) { bs_name =
							 * "N/A"; } else { lac_name =
							 * base_station[0].trim(); } if
							 * (base_station[1].trim().equals("")) { lac_id =
							 * "N/A"; } else { lac_id = base_station[1].trim();
							 * }
							 */
							String bs_name = base_station[0];
							String bs_lac = base_station[1];
							String bs_cel_id = base_station[2];
							Double latitude = Double.parseDouble(base_station[3]);
							Double longitude = Double.parseDouble(base_station[4]);
							deptInputMap.put(bs_cel_id, bs_name + " " + bs_lac + "," + latitude + "," + longitude);
						}
					}
					br.close();
				} else {
					System.out.println("No mapping file exist!!");
				}
			} else {
				System.out.println("No cached file exist!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * get dept detail for each deptid from hashmap and append with emp record,
	 * write in context
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] cdr_data = value.toString().split(",");
			//String cdr_id = deptInputMap.get(empInputs[4]);
			//String empDeptJoinDetail = value + " " + deptDetail;
			//System.out.println(deptDetail);
			//context.write(new Text(empDeptJoinDetail), NullWritable.get());
		} catch (Exception e) {

		}
	}
}
