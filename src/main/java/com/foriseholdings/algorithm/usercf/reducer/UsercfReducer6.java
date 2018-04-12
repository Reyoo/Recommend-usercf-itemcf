package com.foriseholdings.algorithm.usercf.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class UsercfReducer6 extends BaseReducer {

	protected boolean myreduce() {

		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text + ",");
		}
		Configuration conf =context.getConfiguration();
		buss_code = conf.get("buss_code");
		type = conf.get("type");
		String result = null;
		if (sb.toString().endsWith(",")) {
			result = sb.substring(0, sb.length() - 1);
		}

		outKey.set(key);
		//存库中文乱码 原因 暂时没有找到
		outValue.set(result+"	"+type+"	"+ buss_code);
		write();
		return true;
	}
}
