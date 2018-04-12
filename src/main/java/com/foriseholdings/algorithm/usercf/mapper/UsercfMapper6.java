package com.foriseholdings.algorithm.usercf.mapper;

import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.util.PropertyUtil;

public class UsercfMapper6 extends BaseMapper {

	/**
	 * 98
	 * B00003@20128_1.44,B00003@20129_1.44,B00003@20130_1.44,B00003@20132_2.40,B00003@20164_0.48,B00004@20084_2.43,B00004@20086_4.32,B00004@20112_1.44,B00004@20113_9.00,B00004@20164_0.75,B00004@20167_1.44,B00004@CX201801151648288352\20084_0.75,B00004@CX201801161059244951\20088_4.50,B00019@20090_13.21,B00019@20097_3.12,B00019@20098_1.44,B00019@20101_1.74,B00019@20107_15.05,B00019@20109_20.55,B00019@20110_2.00,B00019@20111_1.92,B00019@20112_1.44,B00019@20113_2.40,B00019@20116_1.44,B00019@20119_15.75,B00019@20126_2.75,B00019@20128_0.48,B00019@20129_7.39,B00019@20130_4.80,B00019@20132_23.76,B00019@20138_2.40,B00019@20143_2.43,B00019@20144_2.59,B00019@20145_6.30,B00019@20147_19.26,B00019@20149_0.75,B00019@20156_1.44,B00019@20160_1.92,B00019@20163_33.75,B00019@20164_21.97,B00019@20165_22.32,B00019@20167_8.64,B00019@20168_48.18,B00019@20169_0.75,B00019@20170_4.98,B00019@20172_2.19,B00019@20173_1.44,B00019@20174_4.44,B00019@20175_11.73,B00019@CX201801150943233245\20097_4.50,B00019@CX201801151649399934\20089_12.27,B00019@CX201801181111533940\20113_5.76,B00019@CX201801221028422519\20086_1.50,B00019@CX201801221030244428\20086_0.75,B00019@SP1516005361660\20089_4.50,B00019@SP1516005361692\20085_4.50,B00019@SP1516256830500\20132_4.50,B00019@SP1516590279319\20128_4.50,B00019@ZCX201801151649399934\20089$20089_5.76,B00019@ZCX201801181111533940\20113$20113_2.88
	 * 
	 */
	String top_times = PropertyUtil.getProperty("top_times");
	String prodIDAndScore = null;
	String shopSn = null;

	protected boolean calcProc() {
		// 行
		String userID = value.toString().split("\t")[0];
		// 列_值(数组)
		String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");
		// 每一行长度
		// Map<String, String> store = new HashMap<String, String>();
		// 计算左侧矩阵的空间距离
		for (String column_value : column_value_array_matrix1) {
			shopSn = column_value.split("_")[0].split("@")[0];
			outKey.set(userID + "_" + shopSn);
			prodIDAndScore = column_value.split("@")[1];
			outValue.set(prodIDAndScore);
			write();
		}

		
		
		
		
		
		
		
		
		
		
		
		
		// List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String,
		// String>>(store.entrySet());
		// System.out.println(list.size());
		// Collections.sort(list, new Comparator<Map.Entry<String, String>>() {
		// // 降序排序
		// public int compare(Entry<String, String> o1, Entry<String, String> o2) {
		// return o2.getValue().compareTo(o1.getValue());
		// }
		// });
		// for (Map.Entry<String, String> mapping : list) {
		// if (zero < top_time) {
		// System.out.println(mapping.getKey() + ":" + mapping.getValue());
		// // result 是结果矩阵中的某元素，坐便为 行：row_matrix1 列：row_matrix2 因为右矩阵一已经转置
		// outKey.set(userID); // 用户的ID
		// outValue.set(mapping.getKey() + "_" + mapping.getValue());
		// // 输出格式 用户ID 商品_分值
		// write();
		// zero++;

		return true;
	}
}
