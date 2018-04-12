package com.foriseholdings.algorithm.usercf.reducer;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class UsercfReducer3 extends BaseReducer {

	// key :列号 values

	protected boolean myreduce() {
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			// text :行号_值
			sb.append(text + ",");
		}

		String line = null;
		if (sb.toString().endsWith(",")) {
			line = sb.substring(0, sb.length() - 1);
		}

		outKey.set(key);
		outValue.set(line);
		write();
		return true;

	}

}
