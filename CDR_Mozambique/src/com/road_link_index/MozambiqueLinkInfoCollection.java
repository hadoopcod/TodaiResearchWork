package com.road_link_index;

import com.vividsolutions.jts.geom.Geometry;

public class MozambiqueLinkInfoCollection {
	String[] value;
	Geometry str;
	String name;
	String larger;
	String link_id;
	String road_id;
	String road_name;
	Double start_X;
	Double start_Y;
	Double end_X;
	Double end_Y;
	String start_loc;
	String end_loc;
	String district_name;
	String province_name;

	// String []row;
	
	
	public MozambiqueLinkInfoCollection() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MozambiqueLinkInfoCollection(Geometry str, String name, String larger, String link_id, String road_id,
			String road_name, Double start_X, Double start_Y, Double end_X, Double end_Y, String start_loc,
			String end_loc, String district_name, String province_name) {
		super();
		this.str = str;
		this.name = name;
		this.larger = larger;
		this.link_id = link_id;
		this.road_id = road_id;
		this.road_name = road_name;
		this.start_X = start_X;
		this.start_Y = start_Y;
		this.end_X = end_X;
		this.end_Y = end_Y;
		this.start_loc = start_loc;
		this.end_loc = end_loc;
		this.district_name = district_name;
		this.province_name = province_name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLarger() {
		return larger;
	}

	public void setLarger(String larger) {
		this.larger = larger;
	}

	public String getLink_id() {
		return link_id;
	}

	public void setLink_id(String link_id) {
		this.link_id = link_id;
	}

	public String getRoad_id() {
		return road_id;
	}

	public void setRoad_id(String road_id) {
		this.road_id = road_id;
	}

	public String getRoad_name() {
		return road_name;
	}

	public void setRoad_name(String road_name) {
		this.road_name = road_name;
	}

	public Double getStart_X() {
		return start_X;
	}

	public void setStart_X(Double start_X) {
		this.start_X = start_X;
	}

	public Double getStart_Y() {
		return start_Y;
	}

	public void setStart_Y(Double start_Y) {
		this.start_Y = start_Y;
	}

	public Double getEnd_X() {
		return end_X;
	}

	public void setEnd_X(Double end_X) {
		this.end_X = end_X;
	}

	public Double getEnd_Y() {
		return end_Y;
	}

	public void setEnd_Y(Double end_Y) {
		this.end_Y = end_Y;
	}

	public String getStart_loc() {
		return start_loc;
	}

	public void setStart_loc(String start_loc) {
		this.start_loc = start_loc;
	}

	public String getEnd_loc() {
		return end_loc;
	}

	public void setEnd_loc(String end_loc) {
		this.end_loc = end_loc;
	}

	public String getDistrict_name() {
		return district_name;
	}

	public void setDistrict_name(String district_name) {
		this.district_name = district_name;
	}

	public String getProvince_name() {
		return province_name;
	}

	public void setProvince_name(String province_name) {
		this.province_name = province_name;
	}

}
