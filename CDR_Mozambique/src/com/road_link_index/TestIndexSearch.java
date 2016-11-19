package com.road_link_index;

import java.io.IOException;

import com.vividsolutions.jts.io.ParseException;

public class TestIndexSearch {

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub

		CreateSpatialIndex.buildRticSpatialIndex("/home/cumbane/InputFolder/Road_Input/Road_Network_Moz.csv", "", "");
		
		SearchSpatialIndexPrimary search = new SearchSpatialIndexPrimary();
		
		search.SearchIndexOfPointDataPrimary();
		
	}

}
