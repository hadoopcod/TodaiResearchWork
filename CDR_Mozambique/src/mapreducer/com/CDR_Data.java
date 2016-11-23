package mapreducer.com;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class CDR_Data {

	public String imei_caller;
	public String imsi_caller;
	public String Start_timestamp;
	public String end_timestamp;
	public long lac;
	public String cell_id;
	public String cel_name;
	public double latitude;
	public double longitude;

	public CDR_Data(String row) {
		super();

		String CDR_data[] = row.split(",");

		if(CDR_data.length == 13){
		
		this.imei_caller = CDR_data[2];
		this.imsi_caller = CDR_data[3];
		this.Start_timestamp = CDR_data[4];
		this.end_timestamp = CDR_data[5];
		this.cell_id = CDR_data[6];
		this.cel_name = CDR_data[8];
		this.lac = Integer.parseInt(CDR_data[9]);
		this.latitude = Double.parseDouble(CDR_data[11]);
		this.longitude = Double.parseDouble(CDR_data[12]);
		}
	}
}
