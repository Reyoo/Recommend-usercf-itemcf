package com.foriseholdings.adsLabel.findListFromMysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.foriseholdings.common.util.GetJdbcConn;

/**
 * @author qisun 获取标签值及标签判断条件
 */
public class GetLabelValueMap {



	/**
	 * 根据分类id 获取所有的商品ID
	 * 
	 * @param type_id
	 * @return
	 */
	public static List<String> getGoods(String type_id) {

		List<String> shopSnList = new ArrayList<String>();
		Connection conn = GetJdbcConn.getConnection();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT id,type_id ,product_id,bus_code ,timestrap ,remark from elep_label_product ");
		sb.append(" where type_id = '");
		sb.append(type_id);
		sb.append("'");
		try {
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			pstmt = conn.prepareStatement(sb.toString());
			rs = pstmt.executeQuery();

			while (rs.next()) {
				shopSnList.add(rs.getString("product_id"));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return shopSnList;
	}
}
