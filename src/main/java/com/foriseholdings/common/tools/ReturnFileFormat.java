package com.foriseholdings.common.tools;

import java.util.Date;

public class ReturnFileFormat {

	public static String getPathAddr(String basePath, String orgPath, String busCode) {

		String dateString = DefDateformat.getStringDateShort(new Date());
		String nowPath = null;
		String newOrgPath = null;
		// System.out.println(basePath);

		// if(bus_code.length()==0) {
		//
		// }else {
		//
		// }

		if (basePath.endsWith("/")) {
			basePath = basePath.substring(0, basePath.length() - 1);
		}

		// 此处判断是否有 "/" 如果没有则加上
		if (!orgPath.equals("") && !orgPath.startsWith("/")) {

			orgPath = "/" + orgPath;
		}

		if (orgPath.contains("{#date}")) {
			newOrgPath = orgPath.replace("{#date}", dateString);
			if (newOrgPath.contains("{#busCode}")) {
				newOrgPath = newOrgPath.replace("{#busCode}", busCode);

			}
		}
		if (newOrgPath != null) {
			nowPath = basePath + newOrgPath;
		} else {
			nowPath = basePath + orgPath;
		}

		return nowPath.trim();
	}

	public static void main(String[] args) {
		String a = ReturnFileFormat.getPathAddr("/foriseholdings/Algorithm/", "/applogs/{#date}/", "");
		System.out.println(a);
	}

}
