package com.foriseholdings.parseLogs.reducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ParseReduer2 extends BaseReducer {

	String url = null;
	String userId = null; // 用戶ID
	String shopSn = null; // 门店编码
	String branch = null;
	String score = null;
	String productId = null;
	List<String> prods_list = new ArrayList<String>();
	String laber_id = null;
	String classPath = null;

	protected boolean myreduce() {
		
		if(key.toString() == null ||key.toString().equals("")) {
			System.out.println("reducer is null");
			return false;
		}
		String userIdAndShopSn = key.toString();
		Map<String, Integer> map = new HashMap<String, Integer>();

		for (Text value : values) {
			String shopId = value.toString().split("_")[0];
			String score = value.toString().split("_")[1];

			if (map.get(shopId) == null) {
				map.put(shopId, Integer.valueOf(score));
			} else {
				Integer preScore = map.get(shopId);
				map.put(shopId, preScore + Integer.valueOf(score));
			}

		}

		StringBuilder sBuilder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			String shopId = entry.getKey();
			String score = String.valueOf(entry.getValue());
			sBuilder.append(shopId + "_" + score + ",");

		}
		String line = null;
		// 去掉行末","号
		if (sBuilder.toString().endsWith(",")) {
			line = sBuilder.substring(0, sBuilder.length() - 1);
		}

		// 商品_门店 key
		// 用户_评分 value
		outKey.set(userIdAndShopSn);
		outValue.set(line);
		write();
		return true;
	}

}
