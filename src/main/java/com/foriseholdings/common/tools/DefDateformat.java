package com.foriseholdings.common.tools;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DefDateformat {

	/**
	 * 
	 * @param currentTime
	 * @return yyyyMMdd
	 */
	public static String getStringDateShort(Date currentTime) {
		// Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	/**
	 * 
	 * @param currentTime
	 * @return yyyy-MM-dd
	 */
	public static String getStringDateShort() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	/**
	 * 
	 * @return yyyyMM
	 */
	public static String getStringMonth() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMM");
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	public static String getCurrentTime(String format) {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	// 当前日期 减去一天 返回格式yyyyMMdd
	public static Date subSomeDay(int day) {
		Date currentTime = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentTime);
		calendar.add(Calendar.DAY_OF_MONTH, -day);
		Date subDate = calendar.getTime();
		return subDate;
	}

}
