package com.foriseholdings.adsLabel.findListFromMysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.foriseholdings.adsLabel.findListFromMysql.bean.TargetBean;
import com.foriseholdings.common.util.GetJdbcConn;

public class GetTargetValues {

	public static void main(String[] args) {
		getTarget();
	}

	/**
	 * 獲取所有黑咖啡的商品id
	 * 
	 * @return
	 */
	public static List<List<TargetBean>> getTarget() {
	
			List<TargetBean> classList = null;
			List<List<TargetBean>> result = new ArrayList<List<TargetBean>>();
			Connection conn = GetJdbcConn.getConnection();
			String sql2 = "SELECT portrait_group FROM elep_user_portrait where state = '1'  GROUP BY portrait_group ";
			String sql3 = "SELECT *  FROM elep_user_portrait WHERE   state = '1' and portrait_group = '";
			PreparedStatement pstmt2 = null;
			PreparedStatement pstmt3 = null;
			ResultSet rs1 = null;
			ResultSet rs2 = null;
		
			try {
			pstmt2 = (PreparedStatement) conn.prepareStatement(sql2);
			rs1 = pstmt2.executeQuery();
			while (rs1.next()) {
				String tmpSql = sql3 + rs1.getString("portrait_group") + "'";
				pstmt3 = (PreparedStatement) conn.prepareStatement(tmpSql);
				rs2 = pstmt3.executeQuery();
				classList = new ArrayList<TargetBean>();
				while (rs2.next()) {
					pstmt3 = (PreparedStatement) conn.prepareStatement(tmpSql);
					TargetBean tarBean = new TargetBean();
					tarBean.setId(rs2.getString("id"));
					tarBean.setLabel_id(rs2.getString("portrait_id"));
					tarBean.setPortrait_rate(rs2.getDouble("portrait_rate"));
					tarBean.setPortrait_name(rs2.getString("portrait_name"));
					tarBean.setPortrait_id(rs2.getString("portrait_id"));
					System.out.println(tarBean.getLabel_id());
					classList.add(tarBean);
				}
				result.add(classList);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs1.close();
				if(rs2 != null) {
					rs2.close();
				}
				if(pstmt2 != null) {
					pstmt2.close();
				}
				if(pstmt3 != null) {
					pstmt3.close();
				}
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}

	public static String getUUID(String src_id, String buss_code) {
		String dap_id = null;
		Connection conn = GetJdbcConn.getConnection();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT dap_id FROM ");
		sb.append(" meta_user_mapping ");
		sb.append(" WHERE ");
		sb.append(" src_id = '");
		sb.append(src_id);
		sb.append("' AND ");
		sb.append("bus_code = '");
		sb.append(buss_code);
		sb.append("'");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				dap_id = rs.getString("dap_id");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return dap_id;

	}

}
