package com.foriseholdings.algorithm.Itemcf.mapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.util.PropertyUtil;

public class ItemcfMapper6 extends BaseMapper {
	/**
	 * 目标格式门店，用户作为key 商品id_评分作为 value \ 98
	 * B00019@20089_8.58,B00019@20086_2.70,B00019@CX201801221009133752\20089_8.58,B00004@CX201801161059244951\20088_8.58,B00019@20125_2.70,B00019@CX201801181435322521\20164_2.70,B00019@20090_2.70,B00019@20141_2.70,B00019@20087_8.58,B00019@ZCX201801181435322521\20164$20164_2.70,B00019@20164_6.24
	 * itemCF BC1006
	 * 
	 */
	String top_times = PropertyUtil.getProperty("top_times");
	String shopSns[] = null;

	protected boolean calcProc() {

		// try {
		// // 行
		String user_id = value.toString().split("\t")[0];
		// // 列_值(数组)
		String[] shopSn_prodId_scores = value.toString().split("\t")[1].split(",");
		Map<String, String> store = new HashMap<String, String>();
		for (String shopSn_prodId_score : shopSn_prodId_scores) {
			String shopSn = shopSn_prodId_score.split("@")[0];
			String prodId_score = shopSn_prodId_score.split("@")[1];
			// 如果 value 为空 put
			if (store.get(user_id + "_" + shopSn) == null) {
				store.put(user_id + "_" + shopSn, prodId_score);
			} else {
				prodId_score = store.get(user_id + "_" + shopSn) + "," + prodId_score;
				store.put(user_id + "_" + shopSn, prodId_score);
			}

		}

		Set<String> keySet = store.keySet();

		for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			String value = store.get(key);

			System.out.println(key + "=" + value);

			outKey.set(key); // 用户的ID
			outValue.set(value);
			write();
		}
		return true;
	}

}
