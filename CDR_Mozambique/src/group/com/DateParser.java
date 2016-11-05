package group.com;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateParser {
	
	//Change Unix Epoch Time Stamp to Local Thailand Time Stamp
		public static String parseUnixTimeStamp(Long timestamp)
		{
			SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT+7:00"));	   	   
		    Date date = new Date(timestamp * 1000L);
		    String data_time = dateFormatGmt.format(date);		
			return data_time;
		}
		
		public static Double getUnixTimeDifference(Long s_timestamp, Long e_timestamp) throws ParseException
		{
			SimpleDateFormat dateFormatGmt = new SimpleDateFormat("HH:mm:ss");
		    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT+7:00"));	
		    
		    Date s_date = new Date(s_timestamp * 1000L);	 
		    String s_data_time = dateFormatGmt.format(s_date);	
		    Date s_date_n = dateFormatGmt.parse(s_data_time);
		    
		    Date e_date = new Date(e_timestamp * 1000L);
		    String e_data_time = dateFormatGmt.format(e_date);
		    Date e_date_n = dateFormatGmt.parse(e_data_time);
		    
		    //Time Difference in Minutes
		    Double differnce_time = (double) (e_date_n.getTime() - s_date_n.getTime()) / (60 * 1000);
		   
			return differnce_time;
		}
		
		public static String parseUnixTimeStampLessMinute(Long timestamp)
		{
			SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
		    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT+6:59"));	   	   
		    Date date = new Date(timestamp * 1000L);
		    String data_time = dateFormatGmt.format(date);		
		    
		    
			return data_time;
		}
		
		public static String parseUnixTimeStampMoreMinute(Long timestamp)
		{
			SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
		    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT+7:01"));	   	   
		    Date date = new Date(timestamp * 1000L);
		    String data_time = dateFormatGmt.format(date);		
		    
		    
			return data_time;
		}
		
		public static Long timeToMinutes(String time)
		{		
			String[] hourMin = time.split(":");
			Long hour = Long.parseLong(hourMin[0]);
			Long mins = Long.parseLong(hourMin[1]);
			Long hoursInMins = hour * 60;	
			
			return hoursInMins + mins;		
		}
		
		public static String timeToHour(String _time)
		{
			int time = Integer.parseInt(_time);
			
			int hours = time / 60; //since both are ints, you get an int
			int minutes = time % 60;
			return (hours +":"+ minutes);
		}
		
		public static String timeToSecond(String _time)
		{
			int time = Integer.parseInt(_time);
			
			int hour = (time/3600);
			int minute = (time % 3600)/60;
			int second = (time % 60);
			
			return (hour + ":" + minute + ":" + second);
				
					
		}
		
		public static Long timeToUnixTime(String _time) throws ParseException {
			
			//2016-03-01 08:20:33
			
			SimpleDateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
			long unixtime;
	
			dfm.setTimeZone(TimeZone.getTimeZone("GMT"));// Specify your timezone
	
			unixtime = dfm.parse(_time).getTime();
			unixtime = unixtime / 1000;
	
			return unixtime;
	
		}

}
