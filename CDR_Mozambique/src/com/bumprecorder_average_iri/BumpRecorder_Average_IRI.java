package com.bumprecorder_average_iri;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class BumpRecorder_Average_IRI {
	public static File inputfile = new File("/home/cumbane/share/BumpRecorder/Matched_IRI_Shorted_Link_ID/part-r-00000");
	public static File outputfile = new File("/home/cumbane/share/BumpRecorder/BR_Average_IRI");
	
	public static void main (String []args) throws FileNotFoundException{
	BufferedWriter	bw = null;
	FileWriter 		fw = null;
		try {
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(inputfile));
			String line = br.readLine();
			String []tokens = line.split(";");
			long s_link_id 		=	Long.parseLong(tokens[0].trim());
			double s_iri 		=	Double.parseDouble(tokens[1]);
			String s_line_string =  tokens[2];
			
			double iri_sum		=	0.0;
			double average_iri	=	0.0;
			int count			=	0;
			
			while((line=br.readLine())!=null){
				//System.out.println(s_link_id);
				tokens = line.split(";");
				long e_link_id 	= Long.parseLong(tokens[0].trim());
				double e_iri	 	= Double.parseDouble(tokens[1]);
				String e_line_string = tokens[2];
				
				if(s_link_id == e_link_id){
					iri_sum		+=	s_iri;
					//System.out.println(iri_sum);
					count		+=	1;
					//System.out.println(count);
					s_link_id	= e_link_id;
					s_iri		= e_iri;
					s_line_string = e_line_string;
					
					
				}
				else if(s_link_id != e_link_id){
					
					if(count == 0){
						//System.out.println(s_link_id+","+s_iri);
						fw = new FileWriter(outputfile,true);
						bw = new BufferedWriter(fw);
						bw.write(s_link_id+";"+average_iri+";"+s_line_string);
						bw.newLine();
						bw.close();
						fw.close();
						s_link_id	= e_link_id;
						s_iri		= e_iri;
						s_line_string = e_line_string;
						iri_sum		=	0.0;
						count		=	0;
						average_iri	=	0.0;
					}
					else{
					average_iri	=	iri_sum/count;
					System.out.println(s_link_id+","+average_iri);
					fw = new FileWriter(outputfile,true);
					bw = new BufferedWriter(fw);
					bw.write(s_link_id+";"+average_iri+";"+s_line_string);
					bw.newLine();
					bw.close();
					fw.close();
					s_link_id	= e_link_id;
					s_iri		= e_iri;
					s_line_string = e_line_string;
					iri_sum		=	0.0;
					count		=	0;
					average_iri	=	0.0;
					}
				}
				
				
			}
			average_iri	=	iri_sum/count;
			System.out.println(s_link_id+";"+average_iri+";"+s_line_string);
			fw = new FileWriter(outputfile,true);
			bw = new BufferedWriter(fw);
			bw.write(s_link_id+";"+average_iri+";"+s_line_string);
			bw.newLine();
			bw.close();
			fw.close();
		}
		catch(IOException e){
			
		}
		
}
}
