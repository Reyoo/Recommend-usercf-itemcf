package com.foriseholdings.algorithm.usercf.reducer;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class UsercfReducer1 extends BaseReducer {

	
	protected boolean myreduce() {
		String itemID = key.toString();

		Map<String, Integer> map = new HashMap<String, Integer>();

		for (Text value : values) {

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
		if (sBuilder.toString().endsWith(",")) {
			line = sBuilder.substring(0, sBuilder.length() - 1);
			System.out.println("line-----" + line);
		}
		outKey.set(itemID);
		outValue.set(line);
		// context.write(outKey, outValue);
		write();
		return true;
	}

}
