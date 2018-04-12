package com.foriseholdings.algorithm.Itemcf.reducer;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ItemcfReducer1 extends BaseReducer {

	
	@Override
	protected boolean myreduce() {
//		System.out.println("reducer 执行了");
		String itemID = key.toString();
		// System.out.println("123123"+itemID);
		// <userID,score>
		Map<String, Integer> map = new HashMap<String, Integer>();

		for (Text value : values) {
			// System.out.println("value--->"+ value+"<-------");
			String userID = value.toString().split("_")[0];
			String score = value.toString().split("_")[1];

			if (map.get(userID) == null) {
				map.put(userID, Integer.valueOf(score));
			} else {
				Integer preScore = map.get(userID);
				map.put(userID, preScore + Integer.valueOf(score));
			}

		}

		StringBuilder sBuilder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			String userID = entry.getKey();
			String score = String.valueOf(entry.getValue());
			sBuilder.append(userID + "_" + score + ",");

		}
		String line = null;
		// 去掉行末","号
		if (sBuilder.toString().endsWith(",")) {
			line = sBuilder.substring(0, sBuilder.length() - 1);
		}
		outKey.set(itemID);
		outValue.set(line);
//		System.out.println("reducer---itemID--->"+itemID);
//		System.out.println("reducer---line--->"+line);
		
		
		write();
		// System.out.println("itemID-->"+itemID);
		return true;
	}

}
