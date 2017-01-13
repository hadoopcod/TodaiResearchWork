package com.road_link_index;

import com.vividsolutions.jts.geom.Geometry;

public class MozambiqueLinkInfoCollection {
	
	Geometry str;
	Long edge_id;
	Long start_node;
	//Vertex end_node;
	
	
	
	public MozambiqueLinkInfoCollection() {
		super();
		// TODO Auto-generated constructor stub
	}



	public MozambiqueLinkInfoCollection(Geometry str, Long edge_id, Long start_node) {
		super();
		this.str = str;
		this.edge_id = edge_id;
		this.start_node = start_node;
		//this.end_node = end_node;
	}



	public Geometry getStr() {
		return str;
	}



	public void setStr(Geometry str) {
		this.str = str;
	}



	public Long getEdge_id() {
		return edge_id;
	}



	public void setEdge_id(Long edge_id) {
		this.edge_id = edge_id;
	}



	public Long getStart_node() {
		return start_node;
	}



	public void setStart_node(Long start_node) {
		this.start_node = start_node;
	}



	

}