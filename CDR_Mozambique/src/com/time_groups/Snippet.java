package com.time_groups;

public class Snippet {
	public static int TimeMapInMinutesToHour(Integer time_) {
	
			if (time_ >= 0 && time_ < 60) {
				return 1;
			} else if (time_ >= 60 && time_ < 120) {
				return 2;
			} else if (time_ >= 120 && time_ < 180) {
				return 3;
			} else if (time_ >= 180 && time_ < 240) {
				return 4;
			} else if (time_ >= 240 && time_ < 300) {
				return 5;
			} else if (time_ >= 300 && time_ < 360) {
				return 6;
			} else if (time_ >= 360 && time_ < 420) {
				return 7;
			} else if (time_ >= 420 && time_ < 480) {
				return 8;
			} else if (time_ >= 480 && time_ < 540) {
				return 9;
			} else if (time_ >= 540 && time_ < 600) {
				return 10;
			} else if (time_ >= 600 && time_ < 660) {
				return 11;
			} else if (time_ >= 660 && time_ < 720) {
				return 12;
			} else if (time_ >= 720 && time_ < 780) {
				return 13;
			} else if (time_ >= 780 && time_ < 840) {
				return 14;
			} else if (time_ >= 840 && time_ < 900) {
				return 15;
			} else if (time_ >= 900 && time_ < 960) {
				return 16;
			} else if (time_ >= 960 && time_ < 1020) {
				return 17;
			} else if (time_ >= 1020 && time_ < 1080) {
				return 18;
			} else if (time_ >= 1080 && time_ < 1140) {
				return 19;
			} else if (time_ >= 1140 && time_ < 1200) {
				return 20;
			} else if (time_ >= 1200 && time_ < 1260) {
				return 21;
			} else if (time_ >= 1260 && time_ < 1320) {
				return 22;
			} else if (time_ >= 1320 && time_ < 1380) {
				return 23;
			} else if (time_ >= 1380 && time_ < 1440) {
				return 24;
			}
	
			return 0;
	
		}
	
}

