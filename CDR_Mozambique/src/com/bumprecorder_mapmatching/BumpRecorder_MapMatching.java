package com.bumprecorder_mapmatching;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import jp.ac.ut.csis.pflow.geom.DistanceUtils;
import jp.ac.ut.csis.pflow.geom.LonLat;
import jp.ac.ut.csis.pflow.geom.TrajectoryUtils;
import jp.ac.ut.csis.pflow.routing2.loader.ACsvNetworkLoader.Delimiter;
import jp.ac.ut.csis.pflow.routing2.logic.Dijkstra;
import jp.ac.ut.csis.pflow.routing2.matching.CdrSparseMapMatching;
import jp.ac.ut.csis.pflow.routing2.matching.MatchingResult;
import jp.ac.ut.csis.pflow.routing2.matching.SparseMapMatching;
import jp.ac.ut.csis.pflow.routing2.res.Link;
import jp.ac.ut.csis.pflow.routing2.res.Network;
import jp.ac.ut.csis.pflow.routing2.res.Route;
import jp.utokyo.csis.pflow.mozambique.loader.CsvMozambiqueRoadLoader;
import jp.utokyo.csis.pflow.mozambique.res.MCELCdr;
import jp.utokyo.csis.pflow.mozambique.sample.SparseMapMatchingSample2;



	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileReader;
	import java.io.IOException;
	import java.text.ParseException;
	import java.text.SimpleDateFormat;
	import java.util.ArrayList;
	import java.util.Date;
	import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
	import java.util.Map;
	import java.util.Map.Entry;
	import java.util.TimeZone;

	import org.apache.commons.lang.StringUtils;
	import org.apache.commons.lang.text.StrTokenizer;
	//import org.apache.logging.log4j.LogManager;
	//import org.apache.logging.log4j.Logger;

	import jp.ac.ut.csis.pflow.geom.TrajectoryUtils;
	import jp.ac.ut.csis.pflow.routing2.loader.ACsvNetworkLoader.Delimiter;
	import jp.ac.ut.csis.pflow.routing2.logic.Dijkstra;
	import jp.ac.ut.csis.pflow.routing2.matching.SparseMapMatching;
	import jp.ac.ut.csis.pflow.routing2.res.Link;
	import jp.ac.ut.csis.pflow.routing2.res.Network;
	import jp.ac.ut.csis.pflow.routing2.res.Route;
	import jp.utokyo.csis.pflow.mozambique.loader.CsvMozambiqueRoadLoader;
	import jp.utokyo.csis.pflow.mozambique.res.MCELCdr;
	import jp.ac.ut.csis.pflow.geom.LonLat;
	
	public class BumpRecorder_MapMatching {
		static File 	outputfile= 		new File("/home/cumbane/share/BumpRecorder/BR_MapMatching_semicolon1");
		public static void main(String[] args) throws ParseException, IOException {
			// Mozambique road network /////////////////////////
			
			File    networkFile = 	new File("/home/cumbane/share/BumpRecorder/estradas_network.tsv");
			Network mzNetwork   = loadNetwork(networkFile);
			BufferedWriter bw = null;
			FileWriter fw = null;
			// BumpRecorder  data ///////////////////////////////////////
			File                      iriFile = new File("/home/cumbane/share/BumpRecorder/Surveyed_Road_August_Mesh_Size2.csv");
			Map<Long,List<BumpRecorder>> iris    = loadIRI(iriFile);
			
			Map<String, List<MatchingResult>> average = new HashMap<String, List<MatchingResult>>();
			
			for(Entry<Long,List<BumpRecorder>> entry:iris.entrySet()) {
				
				long        id = entry.getKey();
				List<BumpRecorder> br  = entry.getValue();
				
				for(BumpRecorder b:br){
					
					double longitude =b.getLon();
					double latitude = b.getLat();
					LonLat point = new LonLat(longitude,latitude);
					
					//System.out.println(point.getLon()+","+point.getLat());
					
					MatchingResult res = runMatchingToLink(mzNetwork,point,1000d);
					//System.out.println(id+","+point);
					System.out.println(b.getId()+";"+b.getIri()+";"+res.getNearestPoint().getLon()+";"+res.getNearestPoint().getLat()+";"+res.getNearestLink().getLinkID());
					
					fw = new FileWriter(outputfile,true);
					bw = new BufferedWriter(fw);
					bw.write(b.getId()+";"+b.getIri()+";"+res.getNearestPoint().getLon()+";"+res.getNearestPoint().getLat()+";"+res.getNearestLink().getLinkID()+";"+ TrajectoryUtils.asWKT(res.getNearestLink().getLineString() ));
					bw.newLine();
					bw.close();
					fw.close();
				}
			
			}
				
			
		}
		
		public static MatchingResult runMatchingToLink(Network network,LonLat point,double range) {
			// get candidate links ////////////////////////////
			List<Link> links = network.queryLink(point.getLon(),point.getLat(),range);
			
			// look for nearest link from the candidates //////
			double         distance     = Double.MAX_VALUE;
			Link           nearestLink  = null;
			LonLat         nearestPoint = null;
			Iterator<Link> itr          = links.iterator();
			while(itr.hasNext()) {
				// get candidate link 
				Link candidate = itr.next();
				
				// calculate distance from input point to road link
				LonLat p = DistanceUtils.nearestPoint(candidate.getLineString(),point);
				if( p == null ) { continue; }	// when the nearest point is not found
				
				double d = DistanceUtils.distance(p,point);
				// compare distance
				if( d < distance ) { 
					nearestLink  = candidate;
					nearestPoint = p;
					distance     = d;
				}
			}
			// create matching result and return.
			return new MatchingResult(point,nearestPoint,nearestLink,distance);
		}

		
		/**
		 * load MCEL CDR data
		 * @param cdrFile CDR data
		 * @return CDR data (key=IMEI, value=CDR list)
		 */
		public static Map<Long,List<BumpRecorder>> loadIRI(File iriFile) throws ParseException { 
			// check file existence ////////////////////////////
			if( !iriFile.exists() ) { 
				System.out.println("could not file file::" + iriFile.getAbsolutePath());
				return null;
			}

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			// load MCEL CDRs //////////////////////////////////
			Map<Long,List<BumpRecorder>> br_iris = new HashMap<Long,List<BumpRecorder>>();
			// open CDR file
			try(BufferedReader br=new BufferedReader(new FileReader(iriFile))) {
				String line = br.readLine();	// skip header line
				// check each line 
				while( (line=br.readLine())!=null ) { 
					String[] tokens = StrTokenizer.getCSVInstance(line).getTokenArray();
					
					long id     	 = Long.parseLong(tokens[0]);
					double lat       = Double.parseDouble(tokens[5]);
					double lon       = Double.parseDouble(tokens[6]);
					double iri       = Double.parseDouble(tokens[11]);
					
					BumpRecorder br_iri = new BumpRecorder(id,iri,lon,lat);
					
					if( !br_iris.containsKey(id)) { 
						br_iris.put(id, new ArrayList<BumpRecorder>());
					} 	
					br_iris.get(id).add(br_iri);
				}
			}
			catch(IOException exp) {
				//LOGGER.error("fail to load CDR",exp);
			}
			
			return br_iris;
		}
		
		/**
		 * load road network
		 * @param networkFile network file
		 * @return road network. return null if failed
		 */
		public static Network loadNetwork(File networkFile) { 
			// check file existence ////////////////////////////
			if( !networkFile.exists() ) { 
				System.out.println("could not file file::" + networkFile.getAbsolutePath());
				return null;
			}
			// load OSM network ////////////////////////////////
			return new CsvMozambiqueRoadLoader(networkFile,true,Delimiter.TSV).load();
		}

	}
