package com.road_link_index;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import com.road_link_index.MozambiqueLinkInfoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import com.csvreader.CSVReader;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
//import com.csvreader.CSVWriter;
//import com.gpstaxi.utility.FileWriterUtility;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import com.vividsolutions.jts.io.*;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.operation.buffer.BufferOp;

public class CreateSpatialIndex {

	public static String roadlink_datasource_location;
	public static String primary_indexed_data_location;
	public static String secondary_indexed_data_location;

	public static Envelope minimum_bounding_roadlink_envelope;
	public static STRtree primary_index_mozq;
	public static STRtree secondary_index_rtic;
	
	public static STRtree primary_index_osm;
	public static STRtree secondary_index_osm;
	
	public CreateSpatialIndex() {
		//super();

	}

	public static void buildMinimalBoundingBox(String roadlink_datasource_location_) throws IOException, ParseException{

		roadlink_datasource_location = roadlink_datasource_location_;

		System.out.println("A : ReadCSV");
		
		Path pt=new Path(roadlink_datasource_location);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        CSVReader reader_geometry = new CSVReader(new InputStreamReader((fs.open(pt)), "UTF-8"), ',', '\"', '\'', 1);
		
		//CSVReader reader_geometry = new CSVReader(new InputStreamReader(new FileInputStream(roadlink_datasource_location), "UTF-8"), ',', '\"', '\'', 1);

		String[] values_roadlink_data;
		System.out.println("A : ReadWKT");

		WKTReader wkt_reader = new WKTReader();
		Geometry roadlink_geometry;
		PreparedGeometry roadlink_prepared_geometry;
		GeometryCollection[] roadlink_geometry_collection;

		Envelope roadlink_envelope_collection = new Envelope();

		while ((values_roadlink_data = reader_geometry.readNext()) != null) {

			roadlink_geometry = wkt_reader.read(values_roadlink_data[0]);
			roadlink_envelope_collection.expandToInclude(roadlink_geometry.getEnvelopeInternal());
		}

		minimum_bounding_roadlink_envelope = roadlink_envelope_collection;
		reader_geometry.close();
	}
	
	public static void buildRticSpatialIndex(String roadlink_datasource_location_, String primary_indexed_data_location_,
			String secondary_indexed_data_location_) throws IOException, ParseException{


		roadlink_datasource_location = roadlink_datasource_location_;
		primary_indexed_data_location = primary_indexed_data_location_;
		//secondary_indexed_data_location = secondary_indexed_data_location_;

		//final double DISTANCE_BUFFER_ROAD = 0.000269490; //Appx 30 meter buffer distance 0.000269490
		//final double DISTANCE_BUFFER_ROAD = 0.000089830; //Appx 10 meter buffer distance 0.000089830
		final double PRIMARY_DISTANCE_BUFFER_ROAD = 0.000044915; // Appx 5 meter buffer distance 000044915 10 meter on both side
									

		Path pt=new Path(roadlink_datasource_location);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        CSVReader reader_geometry = new CSVReader(new InputStreamReader((fs.open(pt)), "UTF-8"), ',', '\"', '\'', 1);
		//CSVReader reader_geometry = new CSVReader(new InputStreamReader(new FileInputStream(roadlink_datasource_location), "UTF-8"), ',', '\"', '\'', 1);

		String[] values_roadlink_data;
		WKTReader wkt_reader = new WKTReader();

		Geometry roadlink_geometry;
		PreparedGeometry roadlink_prepared_geometry;
		Envelope roadlink_bound;

		Geometry primary_roadlink_geometry_buffer;
		Envelope primary_roadlink_buffer_bounds;

		Geometry secondary_roadlink_geometry_buffer;
		Envelope secondary_roadlink_buffer_bounds;

		primary_index_mozq = new STRtree();
		//secondary_index_rtic = new STRtree();
		while ((values_roadlink_data = reader_geometry.readNext()) != null) {
			{
				roadlink_geometry = wkt_reader.read(values_roadlink_data[0]);
				roadlink_prepared_geometry = PreparedGeometryFactory.prepare(roadlink_geometry);
				roadlink_bound = roadlink_prepared_geometry.getGeometry().getEnvelopeInternal();

				primary_roadlink_geometry_buffer = BufferOp.bufferOp(roadlink_geometry, PRIMARY_DISTANCE_BUFFER_ROAD);
				//primary_roadlink_buffer_bounds = primary_roadlink_geometry_buffer.getEnvelopeInternal();
	
				//RticLinkInfoCollection primary_rtic_link_info = new RticLinkInfoCollection(values_roadlink_data,primary_roadlink_geometry_buffer);
				//primary_index.insert(primary_roadlink_buffer_bounds, primary_rtic_link_info);
				
				//String name;
				//String larger;
				//String link_id;
				//String road_id;
				//String road_name;
				//String start_X;
				//String start_Y;
				//String end_X;
				//String end_Y;
				//String start_loc;
				//String end_loc;
				//String district_name;
				//String province_name;
				
				MozambiqueLinkInfoCollection road_info = new MozambiqueLinkInfoCollection();
				road_info.setName(values_roadlink_data[1]);
				road_info.setLarger(values_roadlink_data[2]);
				road_info.setLink_id(values_roadlink_data[3]);
				road_info.setRoad_name(values_roadlink_data[4]);
				
				//MozambiqueLinkInfoCollection primary_rtic_link_info = new MozambiqueLinkInfoCollection(values_roadlink_data,roadlink_geometry);
				primary_index_mozq.insert(primary_roadlink_geometry_buffer.getEnvelopeInternal(), road_info);
				
				/*
				secondary_roadlink_geometry_buffer = BufferOp.bufferOp(roadlink_geometry,
						SECONDARY_DISTANCE_BUFFER_ROAD);
				secondary_roadlink_buffer_bounds = secondary_roadlink_geometry_buffer.getEnvelopeInternal();
				RticLinkInfoCollection secondary_rtic_link_info = new RticLinkInfoCollection(values_roadlink_data,
						secondary_roadlink_geometry_buffer);
				
				secondary_index.insert(secondary_roadlink_buffer_bounds, secondary_rtic_link_info);
				*/
			}

		}

		primary_index_mozq.build();
		System.out.println("Primary Index Created");
		//secondary_index.build();
		//System.out.println("Secondary Index Created");

		reader_geometry.close();

		//FileWriterUtility.WriteToFile("primary", primary_indexed_data_location_, primary_index);
		//System.out.println("Primary Index Writen");
		//FileWriterUtility.WriteToFile("secondary", secondary_indexed_data_location_, secondary_index);
		//System.out.println("Secondary Index Writen");
		
	}
	
	/*public static void buildOsmSpatialIndex(String roadlink_datasource_location_, String primary_indexed_data_location_,
			String secondary_indexed_data_location_) throws IOException, ParseException{
		
		roadlink_datasource_location = roadlink_datasource_location_;
		primary_indexed_data_location = primary_indexed_data_location_;
		secondary_indexed_data_location = secondary_indexed_data_location_;
		
		final double PRIMARY_DISTANCE_BUFFER_ROAD = 0.0; // Appx 0 meter buffer distance 000044915 10 meter on both side
		final double SECONDARY_DISTANCE_BUFFER_ROAD = 0.0; // Appx  meter buffer distance 000008983 2 meter on both side
		
		
		Path pt=new Path(roadlink_datasource_location);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        CSVReader reader_geometry = new CSVReader(new InputStreamReader((fs.open(pt)), "UTF-8"), ',', '\"', '\'', 1);
		
		String[] values_roadlink_data;
		WKTReader wkt_reader = new WKTReader();

		Geometry osmlink_geometry;
		PreparedGeometry osmlink_prepared_geometry;
		Envelope osmlink_bound;

		Geometry primary_osmlink_geometry_buffer;
		Envelope primary_osmlink_buffer_bounds;
		
		primary_index_osm = new STRtree();
		while ((values_roadlink_data = reader_geometry.readNext()) != null) {
			
			String wkt = values_roadlink_data[0];
			String id = values_roadlink_data[1];
			String source = values_roadlink_data[2];
			String target = values_roadlink_data[3];
			String kmh = values_roadlink_data[4];
			String linkid = values_roadlink_data[5];
			
			osmlink_geometry = wkt_reader.read(values_roadlink_data[0]);
			osmlink_prepared_geometry = PreparedGeometryFactory.prepare(osmlink_geometry);
			osmlink_bound = osmlink_prepared_geometry.getGeometry().getEnvelopeInternal();
			
			OsmLinkInfoCollection primary_osm_link_info = new OsmLinkInfoCollection(wkt, id, source, target, linkid, kmh, osmlink_geometry);
			
			primary_index_osm.insert(osmlink_bound, primary_osm_link_info);
		
		}
		
		primary_index_osm.build();
		System.out.println("Primary Index Created");
		
		reader_geometry.close();

		//FileWriterUtility.WriteToFile("primary", primary_indexed_data_location_, primary_index_osm);
		//System.out.println("Primary Index Writen");
*/		
	}





