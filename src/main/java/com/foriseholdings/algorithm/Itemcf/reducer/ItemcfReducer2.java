package com.foriseholdings.algorithm.Itemcf.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ItemcfReducer2 extends BaseReducer {
	// private Text outKey = new Text();
	// private Text outValue = new Text();

	protected boolean myreduce() {

		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text + ",");
		}

		Configuration conf = context.getConfiguration();
		buss_code = conf.get("buss_code");
		type = conf.get("type");
		String result = null;
		if (sb.toString().endsWith(",")) {
			result = sb.substring(0, sb.length() - 1);
		}

		outKey.set(key);
		outValue.set(result + "	" + type + "	" + buss_code);
		// context.write(outKey, outValue);
		write();
		return true;

	}
}