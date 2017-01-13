package com.link_average_speed;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;


public class Link {
	Integer link_id;
	String imei;
	Double speed;
	String link;
	
	
	public Link(Integer link_id, String imei, Double speed, String link) {
		super();
		this.link_id = link_id;
		this.imei = imei;
		this.speed = speed;
		this.link = link;
	}


	public Link(String line) throws ParseException{
		String[] tokens = StrTokenizer.getCSVInstance(line).getTokenArray();
		link_id = Integer.parseInt(tokens[0]);
		imei = tokens[1];
		speed = Double.parseDouble(tokens[2]);
		link = tokens[3];
	}
		
	}

