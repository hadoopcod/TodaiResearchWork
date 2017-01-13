package com.link_average_speed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import com.link_average_speed.Link;

public class Link_Speed {
	private static final String filelocation = "/home/cumbane/InputFolder/MapMatchingOutput/Link_Output_File";

	public static void main(String[] args) throws FileNotFoundException, ParseException {
		File file = new File("/home/cumbane/InputFolder/MapMatchingOutput/Link_Output/part-r-00000");
		// TODO Auto-generated method stub
		String link_array[]=null;
		BufferedWriter bw = null;
		FileWriter fw =null;
		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			
			String line = br.readLine();
			link_array = line.split(";");
			int link_id = Integer.parseInt((link_array[0]).trim());
			String imei = link_array[1];
			double speed = Double.parseDouble((link_array[2]).trim());
			String link = link_array[3];
			
			double sum_speed =0.0;
			double average_speed =0.0;
			int count = 0;
			
			while((line = br.readLine())!=null){
				link_array = line.split(";");
				int e_link_id = Integer.parseInt((link_array[0]).trim());
				String e_imei = link_array[1];
				double e_speed = Double.parseDouble((link_array[2]).trim());
				String e_link = link_array[3];
				
				//System.out.println(e_link_id);
				if (link_id == e_link_id && speed < 50){
					sum_speed +=speed;
					count+=1;
					System.out.println(link_id+";"+sum_speed);
					link_id = e_link_id;
					imei = e_imei;
					
					
				}
				
				else if(link_id!=e_link_id){
					average_speed = sum_speed/count;
					System.out.println(link_id+";"+average_speed+";"+link);
					fw = new FileWriter(filelocation,true);
					bw = new BufferedWriter(fw);
					bw.write(link_id+";"+average_speed+";"+link);
					bw.newLine();
					bw.close();
					fw.close();
					sum_speed =0.0;
					average_speed =0.0;
					count = 0;
				}
				
				link_id = e_link_id;
				imei = e_imei;
				speed = e_speed;
				link = e_link;
			}
		
			average_speed = sum_speed/count;
			System.out.println("SSSSSSSSSSSSSSSSSSSSSSSSSSSS: "+link_id+";"+average_speed+";"+link);
			fw = new FileWriter(filelocation,true);
			bw = new BufferedWriter(fw);
			bw.write(link_id+";"+average_speed+";"+link);
			bw.newLine();
			bw.close();
			fw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

}
