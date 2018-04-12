package com.foriseholdings.adsLabel.findListFromMysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.foriseholdings.common.util.GetJdbcConn;

public class GetProdsDivList {

	
	public static void main(String[] args) {
		List<String> testList  ;
		testList = getTarget("test");
		System.out.println(testList.toString());
	}
	
	
	
	/**
	 * 当前版本 V1.0 2018年3月3日 目前版本获取所有商品ID V2.0版本要改成 根据商品名称 获取商品ID的形式
	 * 
	 * @param typeCode
	 *            标签id  现在并没有传值
	 * @return
	 */
	public static List<String> getTarget(String typeCode) {
		List<String> prods_list = new ArrayList<>();
		Connection conn = GetJdbcConn.getConnection();
		// String json = RequestTask.httpURLConectionGET("49.11.11.11");
		// 获取到json
		// String org = "{ \"code\": \"000000\", \"message\": \"业务处理成功！\", \"result\": {
		// \"proIds\": [ \"1050009\", \"10010010\", \"404010008\", \"404010009\",
		// \"404010001\", \"404010003\", \"404010004\", \"404010005\", \"404010006\",
		// \"404010007\", \"404010010\", \"404020002\", \"404020003\", \"404020004\",
		// \"20180116002\", \"KFSP1\", \"KFSP2\", \"KFSP3\", \"KFSP4\", \"404010002\",
		// \"606010001\", \"606010002\", \"606010003\", \"606010094\", \"1040009\",
		// \"1040012\", \"1040011\", \"1040010\", \"1040008\", \"1040007\", \"1030014\",
		// \"1030016\", \"505010001\", \"505010002\", \"505010003\", \"707040001\",
		// \"707040002\", \"707040003\", \"808010001\", \"808010002\", \"808010003\",
		// \"808010004\", \"808010005\", \"808010006\", \"808020001\", \"808020002\",
		// \"808020003\", \"1313010001\", \"1313010002\", \"1313010003\",
		// \"1313010004\", \"1313010005\", \"1313010006\", \"1313010007\",
		// \"1313010008\", \"20180118\", \"20180122001\", \"3010013\", \"3010012\",
		// \"3010019\", \"2020025\", \"2020023\", \"2020003\", \"2020027\", \"2020024\",
		// \"2010006\", \"2010005\", \"2010007\", \"2010004\", \"2010001\", \"2010003\",
		// \"2010002\", \"1050006\", \"1050007\", \"1050010\", \"1050011\", \"1050012\",
		// \"1050013\", \"20180105001\", \"20180116001\", \"1050001\", \"1050003\",
		// \"1050002\", \"1E+12\", \"19880909\", \"3060001\", \"3060002\", \"3060003\",
		// \"3060009\", \"3060005\", \"3060004\", \"3060006\", \"3060010\", \"3050001\",
		// \"3060012\", \"3060014\", \"3060013\", \"3040003\", \"3040001\", \"3040002\",
		// \"3050002\", \"3050003\", \"3060015\", \"3020003\", \"3020002\", \"3020011\",
		// \"3020010\", \"4010039\", \"5010005\" ] } }";
		// ResultBean rb = JSON.parseObject(org, ResultBean.class);

		// 业务处理成功
		// if(rb.getCode().equals("000000")) {
		// prods_list = rb.getResult().getProIds();
		// System.out.println(prods_list.toString());
		//
		// }else {
		// prods_list.add("商品id出错");
		// }
		//
		String sql = "SELECT product_id FROM meta_product ";
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			pstmt = (PreparedStatement) conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int col = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				prods_list.add(rs.getString("product_id"));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				pstmt.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return prods_list;
	}
}
