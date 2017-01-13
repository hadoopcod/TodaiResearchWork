package com.road_link_index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import com.road_link_index.CreateSpatialIndex;
import com.road_link_index.MozambiqueLinkInfoCollection;
//import com.gpstaxi.utility.CSVReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.buffer.BufferOp;

public class SearchSpatialIndexPrimary {

	final double PRIMARY_DISTANCE_BUFFER_ROAD = 0.0044915; //Appx 500 meter buffer distance
	final double SECONDARY_DISTANCE_BUFFER_ROAD = 0.0017966; //Appx 200 meter buffer distance
	final double THIRD_DISTANCE_BUFFER_ROAD = 0.00044915; //Appx 100 meter buffer distance
	//final double SECONDARY_DISTANCE_BUFFER_ROAD = 0.000008983;   //Appx 1 meter buffer distance

	public SearchSpatialIndexPrimary(){
		super();	
	}

	@SuppressWarnings({ "unchecked", "null" })
	public void SearchIndexOfPointDataPrimary()
			throws UnsupportedEncodingException, FileNotFoundException, IOException, ParseException {
		CSVReader reader_cdr_info = new CSVReader(new InputStreamReader(
				new FileInputStream("/home/cumbane/InputFolder/Output_Groupped_Sorted/part-r-00000"), "UTF-8"),
				',', '\"', '\'', 1);
		/*CSVReader reader_taxi_info = new CSVReader(new InputStreamReader(
				new FileInputStream("/media/sf_D_DRIVE/Todai Research/workspace_process_data/InputGpsData/part-00000-15-in-clip-test.csv"), "UTF-8"),
				',', '\"', '\'', 1); */
		
		File file_out_indexed = new File("/home/cumbane/InputFolder/Output_Index/primary_indexed_cdr.csv");
		if (!file_out_indexed.exists()) {
			file_out_indexed.createNewFile();
		}
	
		FileWriter writer_indexed = new FileWriter(file_out_indexed);

		String values_cdr_data[];
		WKTReader wkt_point_reader = new WKTReader();
		Geometry point_geometry = null;
		//PreparedGeometry point_prepared_geometry;
	
		Geometry primary_point_geometry_buffer;
		Envelope primary_point_buffer_bounds;
		String point;
		Geometry secondary_point_geometry_buffer;
		Envelope secondary_point_buffer_bounds;
		Geometry third_point_geometry_buffer;
		Envelope third_point_buffer_bounds;
		String point1;
	
		while ((values_cdr_data = reader_cdr_info.readNext()) != null) {

			//System.out.println(values_cdr_data[8]);
			point = "POINT(" + values_cdr_data[8] + " " + values_cdr_data[9] + ")";
			point_geometry = wkt_point_reader.read(point);
			
			//RticLinkInfoCollection Details For Easy
			/*String mozq_link_data_value[];
			//Geometry rticlink_geometry;
			String WKT   =     mozq_link_data_value[0];
			String mapid =   mozq_link_data_value[1];
			String linkid;  rticlink_data_value[2]
			String nwid;    rticlink_data_value[3]
			String gid;     rticlink_data_value[4]*/
			//String geom;    rticlink_data_value[5]

			primary_point_geometry_buffer = BufferOp.bufferOp(point_geometry, PRIMARY_DISTANCE_BUFFER_ROAD);
			primary_point_buffer_bounds = primary_point_geometry_buffer.getEnvelopeInternal();

			List<MozambiqueLinkInfoCollection> primary_indexed_search_result = CreateSpatialIndex.primary_index_mozq.query(primary_point_buffer_bounds);
			
			secondary_point_geometry_buffer = BufferOp.bufferOp(point_geometry, SECONDARY_DISTANCE_BUFFER_ROAD);
			secondary_point_buffer_bounds = secondary_point_geometry_buffer.getEnvelopeInternal();

			List<MozambiqueLinkInfoCollection> secondary_indexed_search_result = CreateSpatialIndex.secondary_index_mozq.query(secondary_point_buffer_bounds);

			third_point_geometry_buffer = BufferOp.bufferOp(point_geometry, THIRD_DISTANCE_BUFFER_ROAD);
			third_point_buffer_bounds = third_point_geometry_buffer.getEnvelopeInternal();

			List<MozambiqueLinkInfoCollection> third_indexed_search_result = CreateSpatialIndex.third_index_mozq.query(third_point_buffer_bounds);

			
			@SuppressWarnings("unused")
			MozambiqueLinkInfoCollection primary_mozqlink_parameter = null;
			MozambiqueLinkInfoCollection secondary_mozqlink_parameter = null;
			MozambiqueLinkInfoCollection third_mozqlink_parameter = null;
			String[] road_value;
			Geometry primary_rticlink_geometry;
			String searched_rticlink_id;
			Boolean isOnBuffer;
			int indexed_count = 0;
			
			Long edge_id ;
			Long start_node;
			//Long end_node;
			
			
			/*
			 * For Primary Index
			 */
			
			System.out.println(primary_indexed_search_result.size());
			
			
			if (primary_indexed_search_result.size() == 1)
			{
				for (int i = 0; i < primary_indexed_search_result.size(); i++) {
					
					primary_mozqlink_parameter = primary_indexed_search_result.get(i);
					edge_id = primary_mozqlink_parameter.edge_id;
					start_node = primary_mozqlink_parameter.start_node;
					//end_node = primary_mozqlink_parameter.end_node;
					
					indexed_count++;
					
					System.out.println(edge_id  + ";" + start_node );
				
					writer_indexed.write(values_cdr_data[0] + "," + values_cdr_data[1] + "," + values_cdr_data[2] + ","
							+ values_cdr_data[3] + "," + edge_id  + "," + start_node  
							+ "\n");
				}
				
			}
			
			else if(secondary_indexed_search_result.size() == 1)
				{ System.out.println("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS"+secondary_indexed_search_result.size());
					for (int i = 0; i < secondary_indexed_search_result.size(); i++) {
					
					secondary_mozqlink_parameter = secondary_indexed_search_result.get(i);
					edge_id = secondary_mozqlink_parameter.edge_id;
					start_node = secondary_mozqlink_parameter.start_node;
					//end_node = primary_mozqlink_parameter.end_node;
					
					indexed_count++;
					
					System.out.println(edge_id  + ";" + start_node );
				
				//System.out.println(values_cdr_data[0]  + "," + values_cdr_data[1]  + "," + values_cdr_data[2] + "," + values_cdr_data[3] + "," + values_cdr_data[4]+ "," + values_cdr_data[5]+ ";" + values_cdr_data[6]+"," + values_cdr_data[7]+"," + values_cdr_data[8]+"," + values_cdr_data[9]);
					writer_indexed.write(values_cdr_data[0] + "," + values_cdr_data[1] + "," + values_cdr_data[2] + ","
							+ values_cdr_data[3] + "," + edge_id  + "," + start_node  
							+ "\n");
					}
					
			}
			else if(third_indexed_search_result.size() == 1)
			{ System.out.println("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"+third_indexed_search_result.size());
				for (int i = 0; i < third_indexed_search_result.size(); i++) {
				
				third_mozqlink_parameter = third_indexed_search_result.get(i);
				edge_id = third_mozqlink_parameter.edge_id;
				start_node = third_mozqlink_parameter.start_node;
				//end_node = primary_mozqlink_parameter.end_node;
				
				indexed_count++;
				
				System.out.println(edge_id  + ";" + start_node );
			
			//System.out.println(values_cdr_data[0]  + "," + values_cdr_data[1]  + "," + values_cdr_data[2] + "," + values_cdr_data[3] + "," + values_cdr_data[4]+ "," + values_cdr_data[5]+ ";" + values_cdr_data[6]+"," + values_cdr_data[7]+"," + values_cdr_data[8]+"," + values_cdr_data[9]);
				writer_indexed.write(values_cdr_data[0] + "," + values_cdr_data[1] + "," + values_cdr_data[2] + ","
						+ values_cdr_data[3] + "," + edge_id  + "," + start_node  
						+ "\n");
				}
		}
		}
		reader_cdr_info.close();
		writer_indexed.close();
	}

}


