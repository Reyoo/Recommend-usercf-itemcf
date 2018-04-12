package com.foriseholdings.common.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.foriseholdings.common.util.GetJdbcConn;
import com.foriseholdings.common.util.PropertyUtil;

public class JudgeProdStatus {

	/**
	 * @desc 根据业务编码及门店id返回在售商品List
	 * @param busCode
	 *            业务编码 BC1001--D5 、BC1006--咖啡
	 * @param shop_sn
	 *            门店编码
	 * @return
	 */
	public static String getOnSellProdList(String busCode, List<String> shopSnList) {
		JSONObject jobj = new JSONObject();
		String url;
		String username;
		String password;

		// 这个地方如果以后增加多个业务系统会麻烦 要建立不同的数据库链接
		url = PropertyUtil.getProperty(busCode + "jdbc");
		username = PropertyUtil.getProperty(busCode + "username");
		password = PropertyUtil.getProperty(busCode + "password");

		Connection conn = null;
		// conn = GetJdbcConn.getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		conn = GetJdbcConn.getConnection(url, username, password);
		try {
			for (String shopSn : shopSnList) {

				List<String> shopSnProd = new ArrayList<String>();
				StringBuilder sb = new StringBuilder();

				sb.append("SELECT pro_id FROM tb_shop_product WHERE status = '0'");
				sb.append("AND shop_sn=");
				sb.append("'");
				sb.append(shopSn);
				sb.append("'");
				pstmt = conn.prepareStatement(sb.toString());
				rs = pstmt.executeQuery();

				while (rs.next()) {
					shopSnProd.add(rs.getString("pro_id"));
				}
				jobj.put(shopSn, shopSnProd);
				sb.setLength(0);
				pstmt.clearBatch();
				rs.close();

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
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

		}
		return jobj.toJSONString();
	}

	/**
	 * 获取到所有的门店编码list
	 * 
	 * @param args
	 */

	public static List<String> getShopSnList(String busCode) {
		String url;
		String username;
		String password;

		List<String> shopSnList = new ArrayList<String>();
		// 这个地方如果以后增加多个业务系统会麻烦 要建立不同的数据库链接
		url = PropertyUtil.getProperty(busCode + "jdbc");
		username = PropertyUtil.getProperty(busCode + "username");
		password = PropertyUtil.getProperty(busCode + "password");

		Connection conn = null;
		// conn = GetJdbcConn.getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		conn = GetJdbcConn.getConnection(url, username, password);
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT shop_sn FROM tb_shop_product GROUP BY shop_sn ");
		try {
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();

			while (rs.next()) {
				shopSnList.add(rs.getString("shop_sn"));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
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

		}
		return shopSnList;
	}

	public static void main(String[] args) {
	}

}
