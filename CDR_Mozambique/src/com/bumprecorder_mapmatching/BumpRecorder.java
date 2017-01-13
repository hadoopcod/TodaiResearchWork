package com.bumprecorder_mapmatching;

public class BumpRecorder {
	private long id;
	private double iri;
	private double lon;
	private double lat;
	public BumpRecorder(long id, double iri, double lon, double lat) {
		super();
		this.id = id;
		this.iri = iri;
		this.lon = lon;
		this.lat = lat;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public double getIri() {
		return iri;
	}
	public void setIri(double iri) {
		this.iri = iri;
	}
	public double getLon() {
		return lon;
	}
	public void setLon(double lon) {
		this.lon = lon;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	
	
}
