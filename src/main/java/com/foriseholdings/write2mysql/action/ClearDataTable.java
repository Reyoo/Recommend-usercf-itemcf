package com.foriseholdings.write2mysql.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.foriseholdings.common.util.GetJdbcConn;

/*
 * 每日清空数据库表tb_recommend_sort
 * 后期版本应去掉清空功能，
 * target :动态解析数据,存储数据,更新数据记录
 */
public class ClearDataTable {

	public static void main(String[] args) {
//		clearAdsTb("BC1006");
		clearLabelTb("BC1006");
	}
	
	/*
	 * 清空商品推荐 表内容
	 */
	public static boolean clearAdsTb(String busi_code) {

		Statement st = null;
		Connection conn = GetJdbcConn.getConnection();
		String isNull = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			// 执行SQL
			String sql2 = "SELECT count(1) as total FROM elep_shopsn_userid";
			// System.out.println("sql=" + sql);
			pstmt = conn.prepareStatement(sql2.toString());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				isNull = rs.getString("total");
			}

			if (!isNull.equals("0")) {
				st = conn.createStatement();
				String sql = "delete from elep_shopsn_userid where buss_code =" + "'" + busi_code + "'";
				int result = st.executeUpdate(sql);
				// 处理结果
				if (result > 0) {
					System.out.println("刪除操作成功");
					return true;
				} else {
					System.out.println("刪除操作失败");
					return false;
				}
			}else {
				System.out.println("无可删除内容");
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// 释放资源
			// elep_user_label_value

		}
		return true;
	}
	
	
	
	/**
	 * 清空标签表内容
	 * @param busi_code 业务编码
	 * @return
	 */
	public static boolean clearLabelTb(String bus_code) {

		Statement st = null;
		Connection conn = GetJdbcConn.getConnection();
		String isNull = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			// 执行SQL
			String sql2 = "SELECT count(1) as total FROM elep_user_portrait";
			// System.out.println("sql=" + sql);
			pstmt = conn.prepareStatement(sql2.toString());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				isNull = rs.getString("total");
			}

			if (!isNull.equals("0")) {
				st = conn.createStatement();
				String sql = "delete from elep_user_portrait where bus_code =" + "'" + bus_code + "'";
				int result = st.executeUpdate(sql);
				// 处理结果
				if (result > 0) {
					System.out.println("刪除操作成功");
					return true;
				} else {
					System.out.println("刪除操作失败");
					return false;
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (st != null) {
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// 释放资源
			// elep_user_label_value

		}
		return true;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}