package com.road_link_index;

import java.io.IOException;

import com.vividsolutions.jts.io.ParseException;

public class TestIndexSearch {

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub

		CreateSpatialIndex.buildRticSpatialIndex("/home/cumbane/InputFolder/Road_Input/Clean_Road_Network.csv", "", "", "");
		
		SearchSpatialIndexPrimary search = new SearchSpatialIndexPrimary();
		
		search.SearchIndexOfPointDataPrimary();
		
	}

}
