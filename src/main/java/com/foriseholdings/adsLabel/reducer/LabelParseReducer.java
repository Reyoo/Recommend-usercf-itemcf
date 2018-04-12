package com.foriseholdings.adsLabel.reducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class LabelParseReducer extends BaseReducer {

	protected boolean myreduce() {

		String userID = key.toString();
		Map<String, Integer> map = new HashMap<String, Integer>();
		Integer i = 0;
		for (Text value : values) {
			// 输出 商品ID
			String productID = value.toString();

			if (map.get(productID) == null) {
				map.put(productID, Integer.valueOf(++i));
			} else {
				i = map.get(productID);
				map.put(productID, i + Integer.valueOf(1));
			}
			i = 0;
		}
		StringBuilder sBuilder = new StringBuilder();
		for (Entry<String, Integer> entry : map.entrySet()) {
			// userID ：12092_OR200104 商品ID
			String productID = entry.getKey();
			// score ：用户ID是空 是用户ID
			Integer shopTimes = entry.getValue();
			sBuilder.append(productID + "_" + shopTimes + ",");

		}
		String line = null;
		if (sBuilder.toString().endsWith(",")) {
			line = sBuilder.substring(0, sBuilder.length() - 1);
		}
		// 用户，商品，评分
		outKey.set(userID);
		outValue.set(line);
		write();
		return true;

	}

}
