package com.foriseholdings.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GetJdbcConn {

	static String url = PropertyUtil.getProperty("jdbcDri");
	static String username = PropertyUtil.getProperty("username");
	static String password = PropertyUtil.getProperty("password");

	public static Connection getConnection() {
		String driver = "com.mysql.jdbc.Driver";
		Connection conn = null;
		try {
			Class.forName(driver); // classLoader,加载对应驱动
			conn = (Connection) DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Connection getConnection(String url, String username, String password) {
		String driver = "com.mysql.jdbc.Driver";
		Connection conn = null;
		try {
			Class.forName(driver); // classLoader,加载对应驱动
			conn = (Connection) DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

}
