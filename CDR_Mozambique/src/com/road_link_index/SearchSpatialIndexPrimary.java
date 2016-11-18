package com.road_link_index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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
	//final double SECONDARY_DISTANCE_BUFFER_ROAD = 0.000008983;   //Appx 1 meter buffer distance

	public SearchSpatialIndexPrimary(){
		super();	
	}

	@SuppressWarnings("unchecked")
	public void SearchIndexOfPointDataPrimary()
			throws UnsupportedEncodingException, FileNotFoundException, IOException, ParseException {
		CSVReader reader_cdr_info = new CSVReader(new InputStreamReader(
				new FileInputStream("/home/cumbane/InputFolder/Output2/part-r-00000"), "UTF-8"),
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
	
		while ((values_cdr_data = reader_cdr_info.readNext()) != null) {

			//System.out.println(values_cdr_data[8]);
			point = "POINT(" + values_cdr_data[8] + " " + values_cdr_data[9] + ")";
			point_geometry = wkt_point_reader.read(point);
			
			//RticLinkInfoCollection Details For Easy
			//String rticlink_data_value[];
			//Geometry rticlink_geometry;
			//String WKT;     rticlink_data_value[0]
			//String mapid;   rticlink_data_value[1]
			//String linkid;  rticlink_data_value[2]
			//String nwid;    rticlink_data_value[3]
			//String gid;     rticlink_data_value[4]
			//String geom;    rticlink_data_value[5]

			primary_point_geometry_buffer = BufferOp.bufferOp(point_geometry, PRIMARY_DISTANCE_BUFFER_ROAD);
			primary_point_buffer_bounds = primary_point_geometry_buffer.getEnvelopeInternal();

			List<MozambiqueLinkInfoCollection> primary_indexed_search_result = CreateSpatialIndex.primary_index_mozq.query(primary_point_buffer_bounds);

			MozambiqueLinkInfoCollection primary_mozqlink_parameter;
			String[] primary_rticlink_data_value;
			Geometry primary_rticlink_geometry;
			String searched_rticlink_id;
			Boolean isOnBuffer;
			int indexed_count = 0;
			
			String name;
			String larger;
			String link_id;
			String road_id;
			
			/*
			 * For Primary Index
			 */
			
			System.out.println(primary_indexed_search_result.size());
			
			if (primary_indexed_search_result.size() > 0)
			{
				for (int i = 0; i < primary_indexed_search_result.size(); i++) {
					
					primary_mozqlink_parameter = primary_indexed_search_result.get(i);
					
					
					name = primary_mozqlink_parameter.name;
					larger = primary_mozqlink_parameter.larger;
					link_id = primary_mozqlink_parameter.link_id;
					road_id = primary_mozqlink_parameter.road_name;

					indexed_count++;
					
					
					
					System.out.println(name  + ";" + larger  + ";" + link_id + ";" + road_id );
				

					/*
					writer_indexed.write(values_cdr_data[0] + ";" + values_cdr_data[1] + ";" + values_cdr_data[2] + ";"
							+ values_cdr_data[3] + ";" + searched_rticlink_id + ";" + isOnBuffer + ";" + indexed_count
							+ "\n");
							*/
					
				}
			}
			else if(primary_indexed_search_result.size()==0) { 
				
				System.out.println(values_cdr_data[0]  + ";" + values_cdr_data[1]  + ";" + values_cdr_data[2] + ";" + values_cdr_data[3] + ";" + values_cdr_data[4]+ ";" + values_cdr_data[5]+ ";" + values_cdr_data[6]+";" + values_cdr_data[7]+";" + values_cdr_data[8]+";" + values_cdr_data[9]);
				/*writer_indexed.write(values_cdr_data[0] + ";" + values_cdr_data[1] + ";" + values_cdr_data[2] + ";"
						+ values_cdr_data[3] + ";NONE;NONE;NONE" + "\n");*/
			}
		}

		reader_cdr_info.close();
		writer_indexed.close();
	}

}


